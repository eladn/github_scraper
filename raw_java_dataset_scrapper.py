import os
import sys
import math
import asyncio
import argparse
import datetime
import dataclasses
from warnings import warn
from itertools import count
from collections import defaultdict
from urllib.parse import urlparse, parse_qs, urlunparse
from typing import Dict, List, AsyncIterable, Optional, Union

import aiohttp
import aiofile
import aiofiles
import aiopath
from aiofiles import os as aos
from aiofiles import tempfile as atempfile
from aiopath import AsyncPath


@dataclasses.dataclass(frozen=True)
class GithubRepositoryInfo:
    nr_contributors: Optional[int]
    nr_stars: Optional[int]
    nr_commits: Optional[int]
    nr_tags: Optional[int]
    nr_forks: Optional[int]
    nr_branches: Optional[int]
    nr_watchers: Optional[int]
    network_count: Optional[int]
    subscribers_count: Optional[int]
    java_language_freq: Optional[float]
    last_commits_avg_time_delta: Optional[datetime.timedelta]
    default_branch: str


async def _async_copy_file(source_file_path: str, dest_file_path: str, chunk_size: int = 65535):
    async with aiofile.async_open(source_file_path, "rb") as src, \
            aiofile.async_open(dest_file_path, "wb") as dest:
        async for chunk in src.iter_chunked(chunk_size):
            await dest.write(chunk)


class SessionAgent:
    def __init__(self, auth: Optional[aiohttp.BasicAuth] = None, max_nr_concurrent_requests: int = 5, max_nr_attempts: int = 5):
        self.auth = auth
        self.is_under_limit = False
        self.under_limit_wake_up_events = []
        self.concurrent_api_requests_sem = asyncio.BoundedSemaphore(max_nr_concurrent_requests)
        self.max_nr_attempts = max_nr_attempts
        self.session: Optional[aiohttp.ClientSession] = None

    async def create_session(self):
        await self.close_session_if_opened()
        self.session = aiohttp.ClientSession(auth=self.auth)

    async def get_session(self) -> aiohttp.ClientSession:
        while self.session is None:
            await self.create_session()
        return self.session

    async def close_session_if_opened(self):
        session = self.session
        if session is not None:
            self.session = None
            await session.close()

    async def _before_fence(self):
        await self.concurrent_api_requests_sem.acquire()
        if not self.is_under_limit:
            return
        wake_up_events = asyncio.Event()
        self.under_limit_wake_up_events.append(wake_up_events)
        await wake_up_events.wait()

    async def _after_release(self, response_status: int):
        if response_status != 403:
            self.concurrent_api_requests_sem.release()
            return True
        if not self.is_under_limit:
            self.under_limit_wake_up_events = []
            self.is_under_limit = True
            print('Limit reached. Stopping all upcoming requests for some time..')
            await asyncio.sleep(60 * 2)
            print('Woke up.')
            self.is_under_limit = False
            under_limit_wake_up_events = self.under_limit_wake_up_events
            self.under_limit_wake_up_events = []
            for under_limit_wake_up_event in under_limit_wake_up_events:
                under_limit_wake_up_event.set()
        self.concurrent_api_requests_sem.release()
        return False

    async def request(self, url, method='get', json: bool = False, body=None, headers=None):
        for attempt_nr in range(self.max_nr_attempts):
            if attempt_nr > 0:
                await asyncio.sleep(5 * attempt_nr)
            while True:
                await self._before_fence()
                session = await self.get_session()
                request_additional_kwargs = {}
                if body is not None:
                    request_additional_kwargs['body'] = body
                if headers is not None:
                    request_additional_kwargs['headers'] = headers
                async with session.request(method=method, url=url, **request_additional_kwargs) as response:
                    is_limit_ok = await self._after_release(response.status)
                    if not is_limit_ok:
                        continue  # inner loop - don't count as a failed attempt
                    if response.status != 200:
                        break  # like "continue" for outer loop - counts as a failed attempt
                    if json:
                        try:
                            json = await response.json()
                            return response, json
                        except:
                            break  # like "continue" for outer loop - counts as a failed attempt
                    else:
                        return response
        return None


class RawJavaDatasetGitHubScrapper:
    def __init__(
            self, user: Optional[str] = None,
            token: Optional[str] = None,
            max_nr_concurrent_api_requests: int = 5,
            max_concurrent_downloads: int = 5,
            max_nr_attempts: int = 5):
        self.api_session_agent = SessionAgent(
            auth=aiohttp.BasicAuth(user, token) if user and token else None,
            max_nr_concurrent_requests=max_nr_concurrent_api_requests,
            max_nr_attempts=max_nr_attempts)
        self.downloads_session_agent = SessionAgent(
            max_nr_concurrent_requests=max_concurrent_downloads,
            max_nr_attempts=max_nr_attempts)

    def is_repo_considered_popular(self, repo_info: GithubRepositoryInfo) -> bool:
        repo_info_min_requirement = GithubRepositoryInfo(
            nr_contributors=50,
            nr_stars=100,
            nr_commits=1500,
            nr_tags=10,
            nr_forks=30,
            nr_branches=10,
            nr_watchers=30,
            network_count=20,
            subscribers_count=30,
            java_language_freq=0.7,
            last_commits_avg_time_delta=datetime.timedelta(days=-60),
            default_branch=''
        )
        is_repo_popular = all(
            getattr(repo_info, field.name) >= getattr(repo_info_min_requirement, field.name)
            for field in dataclasses.fields(repo_info_min_requirement)
            if getattr(repo_info, field.name) is not None and
            isinstance(getattr(repo_info_min_requirement, field.name), (int, float, datetime.timedelta)))
        # print('is_repo_popular', is_repo_popular)
        return is_repo_popular

    async def find_all_java_repositories_of_owner(self, owner_name: str) -> AsyncIterable[dict]:
        api_repos_req_url = f'https://api.github.com/orgs/{owner_name}/repos'
        async for repo_dict in self._paginated_iterator_api_call(api_repos_req_url):
            assert 'name' in repo_dict and 'language' in repo_dict
            if repo_dict['language'] and repo_dict['language'].lower() == 'java':
                yield repo_dict

    async def find_all_java_repositories_names_of_owner(self, owner_name: str) -> AsyncIterable[str]:
        api_repos_req_url = f'https://api.github.com/orgs/{owner_name}/repos'
        async for repo_dict in self._paginated_iterator_api_call(api_repos_req_url):
            assert 'name' in repo_dict and 'language' in repo_dict
            if repo_dict['language'] and repo_dict['language'].lower() == 'java':
                yield repo_dict['name']

    def _get_url_for_page(self, api_req_url: str, page_size: int, page_nr: int) -> str:
        api_req_url_parsed = urlparse(api_req_url)
        querystring_dict = parse_qs(api_req_url_parsed.query)
        if 'per_page' in querystring_dict or 'page' in querystring_dict:
            warn('Given API request url should not include `page` or `per_page` query params.')
        querystring_dict['page'] = [f'{page_nr}']
        querystring_dict['per_page'] = [f'{page_size}']
        querystring = '&'.join(f'{k}={val}' for k, values in querystring_dict.items() for val in values)
        api_req_url_parsed = api_req_url_parsed._replace(query=querystring)
        api_req_url_for_cur_page = urlunparse(api_req_url_parsed)
        return api_req_url_for_cur_page

    def _extract_nr_pages_from_paginated_list_api_call_response(self, response: aiohttp.ClientResponse) -> int:
        if 'Link' not in response.headers:
            return 1
        last_page_url = response.headers.get('Link').split(',')[1].split(';')[0].split('<')[1].split('>')[0]
        last_page_nr = int(parse_qs(urlparse(last_page_url).query)['page'][0])
        assert last_page_nr >= 1
        return last_page_nr

    async def _get_nr_pages_for_paginated_api_call(self, api_req_url: str) -> Optional[int]:
        first_page_url = self._get_url_for_page(
            api_req_url=api_req_url, page_size=1, page_nr=1)
        first_page_res = await self._api_call(first_page_url)
        if first_page_res is None:
            return None
        return self._extract_nr_pages_from_paginated_list_api_call_response(first_page_res) + 1

    async def _paginated_list_api_call(
            self, api_req_url: str,
            page_size: int = 100,
            max_nr_items: Optional[int] = None,
            nr_pages_prefetch: int = 1) -> List[dict]:
        return [item async for item in self._paginated_iterator_api_call(
                    api_req_url=api_req_url,
                    page_size=page_size,
                    max_nr_items=max_nr_items,
                    nr_pages_prefetch=nr_pages_prefetch)]

    async def _paginated_iterator_api_call(
            self, api_req_url: str,
            page_size: int = 100,
            max_nr_items: Optional[int] = None,
            nr_pages_prefetch: int = 1) -> AsyncIterable[dict]:
        if max_nr_items is not None:
            assert max_nr_items > 0
            page_size = min(page_size, max_nr_items)

        nr_yielded_items = 0
        if nr_pages_prefetch == 0:
            for page_nr in count(start=1):
                if max_nr_items is not None and nr_yielded_items >= max_nr_items:
                    return
                res_json = await self._json_api_call(self._get_url_for_page(
                    api_req_url=api_req_url, page_size=page_size, page_nr=page_nr))
                if res_json is None:
                    raise RuntimeError(f'Could not fetch page {page_nr}.')
                assert isinstance(res_json, list)
                if len(res_json) < 1:
                    return  # previous page was the last one
                for item in res_json:
                    nr_yielded_items += 1
                    yield item
                    if max_nr_items is not None and nr_yielded_items >= max_nr_items:
                        return
        else:
            assert nr_pages_prefetch > 0
            first_page_res_task = asyncio.create_task(self._api_call(self._get_url_for_page(
                api_req_url=api_req_url, page_size=page_size, page_nr=1)))
            first_page_res = await first_page_res_task
            last_page_nr = self._extract_nr_pages_from_paginated_list_api_call_response(first_page_res)
            if max_nr_items is not None:
                last_page_nr = min(last_page_nr, int(math.ceil(max_nr_items / page_size)))
            pages_tasks: Dict[int, asyncio.Task] = {1: first_page_res_task}
            for page_nr in range(1, last_page_nr + 1):
                if max_nr_items is not None and nr_yielded_items >= max_nr_items:
                    return
                # prefetch next items
                for page_nr_to_prefetch in range(page_nr + 1, min(page_nr + 1 + nr_pages_prefetch, last_page_nr + 1)):
                    assert page_nr_to_prefetch not in pages_tasks
                    pages_tasks[page_nr_to_prefetch] = \
                        asyncio.create_task(self._json_api_call(self._get_url_for_page(
                            api_req_url=api_req_url, page_size=page_size, page_nr=page_nr_to_prefetch)))
                assert page_nr in pages_tasks
                page_task = pages_tasks[page_nr]
                page_res = page_task.result() if page_task.done() else await page_task  # avoid awaiting twice
                if page_res is None:
                    # Prefetch failed (maybe too many attempts). Here we give it a second change.
                    page_res = await self._json_api_call(self._get_url_for_page(
                        api_req_url=api_req_url, page_size=page_size, page_nr=page_nr))
                    if page_res is None:
                        raise RuntimeError(f'Could not fetch page {page_nr}.')
                # The first page result is a response (we needed the header) - handle it correctly.
                if isinstance(page_res, aiohttp.ClientResponse):
                    assert page_nr == 1
                    try:
                        res_json = await page_res.json()
                    except:
                        page_res = await self._json_api_call(self._get_url_for_page(
                            api_req_url=api_req_url, page_size=page_size, page_nr=page_nr))
                        if page_res is None:
                            raise RuntimeError(f'Could not fetch page {page_nr}.')
                        res_json = page_res
                else:
                    res_json = page_res
                # res_json = await page_res.json() if isinstance(page_res, aiohttp.ClientResponse) else page_res
                assert res_json is not None
                assert isinstance(res_json, list)
                assert len(res_json) > 0
                for item in res_json:
                    nr_yielded_items += 1
                    yield item
                    if max_nr_items is not None and nr_yielded_items >= max_nr_items:
                        return

    async def _get_item_by_index_from_paginated_list_api_call(
            self, api_req_url: str, item_index: int) -> Optional[Union[dict, list]]:
        first_page_res = await self._api_call(self._get_url_for_page(
            api_req_url=api_req_url, page_size=1, page_nr=1))
        if first_page_res is None:
            return None
        last_page_nr = self._extract_nr_pages_from_paginated_list_api_call_response(first_page_res)
        wanted_page_nr = last_page_nr - item_index
        assert wanted_page_nr >= 1
        wanted_page_res = await self._json_api_call(self._get_url_for_page(
            api_req_url=api_req_url, page_size=1, page_nr=wanted_page_nr))
        assert isinstance(wanted_page_res, list)
        assert len(wanted_page_res) == 1
        return wanted_page_res[0]

    async def _json_api_call(self, api_req_url: str) -> Optional[Union[list, dict]]:
        ret = await self.api_session_agent.request(api_req_url, json=True)
        if ret is None:
            # TODO: what do we do in this case?
            return None
        print('successful json api request')
        resp, json = ret
        return json

    async def _api_call(self, api_req_url: str) -> Optional[aiohttp.ClientResponse]:
        response = await self.api_session_agent.request(api_req_url)
        if response is None:
            # TODO: what do we do in this case?
            pass
        if response is not None:
            print('successful api request')
        return response

    async def _get_repository_nr_commits_old(self, owner_name: str, repository_name: str) -> int:
        api_info_req_url = f'https://api.github.com/repos/{owner_name}/{repository_name}'
        last_commit_sha = [item async for item in self._paginated_iterator_api_call(
            f'{api_info_req_url}/commits', page_size=1, max_nr_items=1)][0]['sha']
        first_commit = await self._get_item_by_index_from_paginated_list_api_call(
            f'{api_info_req_url}/commits', item_index=0)
        first_commit_sha = first_commit['sha']
        commits_compare_url = \
            f'https://api.github.com/repos/{owner_name}/{repository_name}/compare/' \
            f'{first_commit_sha}...{last_commit_sha}'
        commits_compare = await self._json_api_call(commits_compare_url)
        assert 'total_commits' in commits_compare
        return int(commits_compare['total_commits']) + 1

    async def get_repository_nr_commits(self, owner_name: str, repository_name: str) -> int:
        api_info_req_url = f'https://api.github.com/repos/{owner_name}/{repository_name}'
        return await self._get_nr_pages_for_paginated_api_call(f'{api_info_req_url}/commits')

    async def get_repository_nr_branches(self, owner_name: str, repository_name: str) -> int:
        api_info_req_url = f'https://api.github.com/repos/{owner_name}/{repository_name}'
        return await self._get_nr_pages_for_paginated_api_call(f'{api_info_req_url}/branches')

    async def get_repository_nr_contributors(self, owner_name: str, repository_name: str) -> int:
        api_info_req_url = f'https://api.github.com/repos/{owner_name}/{repository_name}'
        return await self._get_nr_pages_for_paginated_api_call(f'{api_info_req_url}/contributors')

    async def get_repository_nr_tags(self, owner_name: str, repository_name: str) -> int:
        api_info_req_url = f'https://api.github.com/repos/{owner_name}/{repository_name}'
        return await self._get_nr_pages_for_paginated_api_call(f'{api_info_req_url}/tags')

    async def get_repository_default_branch(self, owner_name: str, repository_name: str) -> Optional[str]:
        api_info_req_url = f'https://api.github.com/repos/{owner_name}/{repository_name}'
        repo_info_dict = await self._json_api_call(api_info_req_url)
        if repo_info_dict is None:
            return None
        return repo_info_dict['default_branch']

    async def get_repository_info(
            self, owner_name: str, repository_name: str,
            repo_dict: Optional[dict] = None) -> Optional[GithubRepositoryInfo]:
        api_info_req_url = f'https://api.github.com/repos/{owner_name}/{repository_name}'
        requests_coroutines = {
            'repo_languages_dict': self._json_api_call(f'{api_info_req_url}/languages'),
            'repo_nr_contributors': self.get_repository_nr_contributors(
                owner_name=owner_name, repository_name=repository_name),
            # 'repo_nr_tags': self.get_repository_nr_tags(
            #     owner_name=owner_name, repository_name=repository_name),
            'repo_last_commits': self._paginated_list_api_call(f'{api_info_req_url}/commits', max_nr_items=20),
            'repo_nr_commits': self.get_repository_nr_commits(
                owner_name=owner_name, repository_name=repository_name),
            # 'repo_nr_branches': self.get_repository_nr_branches(
            #     owner_name=owner_name, repository_name=repository_name)
        }
        if repo_dict is None:
            requests_coroutines['repo_info_dict'] = self._json_api_call(api_info_req_url)
        # invoke all together and wait for all to finish
        requests_tasks = {name: asyncio.create_task(coroutine) for name, coroutine in requests_coroutines.items()}
        await asyncio.gather(*requests_tasks.values())
        responses = {name: task.result() for name, task in requests_tasks.items()}
        repo_info_dict = responses['repo_info_dict'] if 'repo_info_dict' in responses else repo_dict
        repo_languages_dict = responses['repo_languages_dict']
        repo_nr_contributors = responses['repo_nr_contributors']
        # repo_nr_tags = responses['repo_nr_tags']
        repo_last_commits = responses['repo_last_commits']
        repo_nr_commits = responses['repo_nr_commits']
        # repo_nr_branches = responses['repo_nr_branches']

        last_commits_datetimes = [
            datetime.datetime.strptime(commit['commit']['committer']['date'], "%Y-%m-%dT%H:%M:%SZ")
            for commit in repo_last_commits]  # 2021-09-23T05:14:39Z
        last_commits_dates_avg_datetime = datetime.datetime.fromtimestamp(
            sum(map(datetime.datetime.timestamp, last_commits_datetimes)) / len(last_commits_datetimes))
        last_commits_avg_time_delta = last_commits_dates_avg_datetime - datetime.datetime.today()
        if repo_info_dict is None:
            return None
        if repo_languages_dict is None:
            return None
        java_language_freq = \
            (repo_languages_dict['Java'] / sum(repo_languages_dict.values())) if 'Java' in repo_languages_dict else 0
        return GithubRepositoryInfo(
            nr_contributors=repo_nr_contributors,
            nr_stars=repo_info_dict['stargazers_count'],
            nr_commits=repo_nr_commits,
            # nr_tags=repo_nr_tags,
            nr_tags=None,
            nr_forks=repo_info_dict['forks_count'],
            # nr_branches=repo_nr_branches,
            nr_branches=None,
            nr_watchers=repo_info_dict['watchers_count'],
            network_count=None,  #repo_info_dict['network_count'],
            subscribers_count=None,  #repo_info_dict['subscribers_count'],
            java_language_freq=java_language_freq,
            last_commits_avg_time_delta=last_commits_avg_time_delta,
            default_branch=repo_info_dict['default_branch']
        )

    async def scrape_and_prepare_owners(
            self, owner_names: List[str], output_dir_path: str, popularity_check: bool = True):
        tasks = []
        for owner_name in owner_names:
            tasks.append(asyncio.create_task(self.scrape_and_prepare_owner(
                owner_name=owner_name, output_dir_path=output_dir_path, popularity_check=popularity_check)))
            # Yield to avoid starving of sub-tasks. Prefer depth-first work-order scheduling over breadth-first.
            # Namely, allow download of the 1st project before finishing scraping data on all projects.
            await asyncio.sleep(1)
        await asyncio.gather(*tasks)

    async def scrape_and_prepare_owner(
            self, owner_name: str, output_dir_path: str, popularity_check: bool = True):
        tasks = []
        async for repository_dict in self.find_all_java_repositories_of_owner(owner_name=owner_name):
            if popularity_check:
                tasks.append(asyncio.create_task(self.scrape_and_prepare_repository_if_popular(
                    owner_name=owner_name, repository_name=repository_dict['name'],
                    output_dir_path=output_dir_path, repository_dict=repository_dict)))
            else:
                tasks.append(asyncio.create_task(self.scrape_and_prepare_repository(
                    owner_name=owner_name, repository_name=repository_dict['name'],
                    output_dir_path=output_dir_path, branch_name=repository_dict['default_branch'])))
            # Yield to avoid starving of sub-tasks. Prefer depth-first work-order scheduling over breadth-first.
            # Namely, allow download of the 1st project before finishing scraping data on all projects.
            await asyncio.sleep(1)
        await asyncio.gather(*tasks)

    async def scrape_and_prepare_repository_if_popular(
            self, owner_name: str, repository_name: str, output_dir_path: str,
            repository_dict: Optional[dict] = None):
        repository_info = await self.get_repository_info(
            owner_name=owner_name, repository_name=repository_name, repo_dict=repository_dict)
        if self.is_repo_considered_popular(repo_info=repository_info):
            await self.scrape_and_prepare_repository(
                owner_name=owner_name, repository_name=repository_name,
                branch_name=repository_info.default_branch, output_dir_path=output_dir_path)

    async def scrape_and_prepare_repository(
            self, owner_name: str, repository_name: str, output_dir_path: str,
            branch_name: Optional[str] = None):
        if branch_name is None:
            branch_name = await self.get_repository_default_branch(
                owner_name=owner_name, repository_name=repository_name)
        async with atempfile.TemporaryDirectory() as tmp_dir:
            await self.clone_repository(
                owner_name=owner_name, repository_name=repository_name,
                branch_name=branch_name, target_dir_path=tmp_dir)
            repo_output_dir = os.path.join(output_dir_path, repository_name)
            repo_output_dir_path = aiopath.path.AsyncPath(repo_output_dir)
            await repo_output_dir_path.mkdir(parents=True, exist_ok=True)
            await self.recursively_find_and_copy_all_java_files(
                look_in_dir_path=tmp_dir, tgt_dir_path=repo_output_dir)

    async def clone_repository(
            self, owner_name: str, repository_name: str, branch_name: str,
            target_dir_path: str, chunk_size: int = 65535):
        repo_zip_file_url = f'https://github.com/{owner_name}/{repository_name}/archive/{branch_name}.zip'
        response = await self.downloads_session_agent.request(repo_zip_file_url)
        if response is None:
            pass
        print('successful file download')
        zip_file_path = os.path.join(target_dir_path, f'{repository_name}.zip')
        async with aiofiles.open(zip_file_path, mode='wb') as zip_file:
            while True:
                chunk = await response.content.read(chunk_size)
                if not chunk:
                    break
                await zip_file.write(chunk)
        unzip_proc = await asyncio.create_subprocess_exec(
            'unzip', '-o', zip_file_path, '-d', target_dir_path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL)
        await unzip_proc.communicate()
        exit_code = await unzip_proc.wait()
        assert exit_code == 0
        return

    async def recursively_find_and_copy_all_java_files(self, look_in_dir_path: str, tgt_dir_path: str):
        filename_occurrences: Dict[str, int] = defaultdict(int)
        tasks = []
        async for java_file_path in AsyncPath(look_in_dir_path).glob('**/*.java'):
            filename = os.path.basename(java_file_path)
            filename_occurrences[filename] += 1
            if filename_occurrences[filename] > 1:
                filename = f"{filename.rstrip('.java')}__{filename_occurrences[filename]}.java"
                assert filename not in filename_occurrences
            tasks.append(asyncio.create_task(_async_copy_file(
                source_file_path=java_file_path,
                dest_file_path=os.path.join(tgt_dir_path, filename))))
        await asyncio.gather(*tasks)

    async def close(self):
        await asyncio.gather((
            asyncio.create_task(self.api_session_agent.close_session_if_opened()),
            asyncio.create_task(self.downloads_session_agent.close_session_if_opened())))


async def async_main():
    parser = create_argparser()
    args = parser.parse_args()
    scrapper = RawJavaDatasetGitHubScrapper(user=args.user, token=args.token)
    try:
        await scrapper.scrape_and_prepare_owners(
            owner_names=args.owner_names, output_dir_path=args.output_dir_path,
            popularity_check=not args.no_popularity_check)
    finally:
        await scrapper.close()


def sync_main():
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(async_main())


def create_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--owners', type=str, required=True, nargs='+', dest='owner_names')
    parser.add_argument('--no-popularity-check', action='store_true', dest='no_popularity_check')
    parser.add_argument('--output-dir', type=str, required=True, dest='output_dir_path')
    parser.add_argument('--user', type=str, dest='user')
    parser.add_argument('--token', type=str, dest='token')
    return parser


if __name__ == '__main__':
    sync_main()
