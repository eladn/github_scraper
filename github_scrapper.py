import os
import sys
import math
import asyncio
import argparse
import datetime
from warnings import warn
from itertools import count
from contextlib import asynccontextmanager
from urllib.parse import urlparse, parse_qs, urlunparse
from typing import Dict, List, AsyncIterable, Optional, Union

import aiohttp
import aiofiles
import aiopath
from aiofiles import tempfile as atempfile

from session_agent import SessionAgent
from async_io_file_helper import AsyncIOFileHelper
from github_repository_info import GithubRepositoryInfo
from github_repository_popularity import is_repository_considered_popular


"""
TODO:
- progress-bar
- saving work status
- catch terminate signal and peacefully exit
"""


class GitHubScraper:
    def __init__(
            self, user: Optional[str] = None,
            token: Optional[str] = None,
            max_nr_concurrent_api_requests: int = 5,
            max_concurrent_downloads: int = 5,
            max_nr_concurrent_fs_ios: int = 10,
            max_nr_attempts: int = 5):
        self.max_nr_attempts = max_nr_attempts
        self.api_session_agent = SessionAgent(
            auth=aiohttp.BasicAuth(user, token) if user and token else None,
            max_nr_concurrent_requests=max_nr_concurrent_api_requests,
            max_nr_attempts=max_nr_attempts)
        self.downloads_session_agent = SessionAgent(
            max_nr_concurrent_requests=max_concurrent_downloads,
            max_nr_attempts=max_nr_attempts)
        self.ioFileHelper = AsyncIOFileHelper(max_nr_concurrent_operations=max_nr_concurrent_fs_ios)
        self.concurrent_fs_io_processes_sem = asyncio.BoundedSemaphore(max_nr_concurrent_fs_ios)

    async def find_all_repositories_of_owner(self, owner_name: str) -> AsyncIterable[dict]:
        api_repos_req_url = f'https://api.github.com/orgs/{owner_name}/repos'
        async for repo_dict in self._paginated_iterator_api_call(api_repos_req_url):
            assert 'name' in repo_dict
            yield repo_dict

    async def find_all_repositories_names_of_owner(self, owner_name: str) -> AsyncIterable[str]:
        api_repos_req_url = f'https://api.github.com/orgs/{owner_name}/repos'
        async for repo_dict in self._paginated_iterator_api_call(api_repos_req_url):
            assert 'name' in repo_dict
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

    async def _get_nr_items_for_paginated_api_call(
            self, api_req_url: str, raise_on_failure: bool = True) -> Optional[int]:
        return await self._get_nr_pages_for_paginated_api_call(
            api_req_url=api_req_url, page_size=1, raise_on_failure=raise_on_failure)

    async def _get_nr_pages_for_paginated_api_call(
            self, api_req_url: str, page_size: int = 1, raise_on_failure: bool = True) -> Optional[int]:
        first_page_url = self._get_url_for_page(
            api_req_url=api_req_url, page_size=page_size, page_nr=1)
        async with self._api_call(first_page_url, raise_on_failure=raise_on_failure) as first_page_res:
            if first_page_res is None:
                assert not raise_on_failure
                return None
            return self._extract_nr_pages_from_paginated_list_api_call_response(first_page_res)

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
                    api_req_url=api_req_url, page_size=page_size, page_nr=page_nr), raise_on_failure=True)
                assert res_json is not None
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
            first_page_url = self._get_url_for_page(api_req_url=api_req_url, page_size=page_size, page_nr=1)
            async with self._api_call(first_page_url, json=True, raise_on_failure=True) as first_page_ret:
                assert first_page_ret is not None
                first_page_res, first_page_json = first_page_ret
                last_page_nr = self._extract_nr_pages_from_paginated_list_api_call_response(first_page_res)
                first_page_res_task = asyncio.create_task(_pre_solved_coroutine(first_page_json))

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
                            api_req_url=api_req_url, page_size=page_size, page_nr=page_nr_to_prefetch),
                            raise_on_failure=False))
                assert page_nr in pages_tasks
                page_task = pages_tasks[page_nr]
                page_json_res = page_task.result() if page_task.done() else await page_task  # avoid awaiting twice
                if page_json_res is None:
                    # Prefetch failed. Here we give it a second change.
                    page_json_res = await self._json_api_call(self._get_url_for_page(
                        api_req_url=api_req_url, page_size=page_size, page_nr=page_nr), raise_on_failure=True)
                    assert page_json_res is not None
                assert page_json_res is not None
                assert isinstance(page_json_res, list)
                assert len(page_json_res) > 0
                for item in page_json_res:
                    nr_yielded_items += 1
                    yield item
                    if max_nr_items is not None and nr_yielded_items >= max_nr_items:
                        return

    async def _get_item_by_index_from_paginated_list_api_call(
            self, api_req_url: str, item_index: int, raise_on_failure: bool = True) -> Optional[Union[dict, list]]:
        last_page_nr = await self._get_nr_items_for_paginated_api_call(
            api_req_url=api_req_url, raise_on_failure=raise_on_failure)
        wanted_page_nr = last_page_nr - item_index
        assert wanted_page_nr >= 1
        wanted_page_url = self._get_url_for_page(
            api_req_url=api_req_url, page_size=1, page_nr=wanted_page_nr)
        wanted_page_res = await self._json_api_call(wanted_page_url, raise_on_failure=raise_on_failure)
        if wanted_page_res is None:
            assert not raise_on_failure
            return None
        assert isinstance(wanted_page_res, list)
        assert len(wanted_page_res) == 1
        return wanted_page_res[0]

    async def _json_api_call(self, api_req_url: str, raise_on_failure: bool = True) -> Optional[Union[list, dict]]:
        async with self.api_session_agent.request(api_req_url, json=True, raise_on_failure=raise_on_failure) as ret:
            if ret is None:
                assert not raise_on_failure
                return None
            print('successful json api request')
            resp, json = ret
            return json

    @asynccontextmanager
    async def _api_call(self, api_req_url: str, json: bool = False, raise_on_failure: bool = True) \
            -> Optional[aiohttp.ClientResponse]:
        async with self.api_session_agent.request(api_req_url, json=json, raise_on_failure=raise_on_failure) as ret:
            if ret is None:
                assert not raise_on_failure
                yield None
                return
            print('successful api request')
            yield ret

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
        commits_compare = await self._json_api_call(commits_compare_url, raise_on_failure=True)
        assert 'total_commits' in commits_compare
        return int(commits_compare['total_commits']) + 1

    async def get_repository_nr_commits(self, owner_name: str, repository_name: str) -> int:
        api_info_req_url = f'https://api.github.com/repos/{owner_name}/{repository_name}'
        return await self._get_nr_items_for_paginated_api_call(f'{api_info_req_url}/commits')

    async def get_repository_nr_branches(self, owner_name: str, repository_name: str) -> int:
        api_info_req_url = f'https://api.github.com/repos/{owner_name}/{repository_name}'
        return await self._get_nr_items_for_paginated_api_call(f'{api_info_req_url}/branches')

    async def get_repository_nr_contributors(self, owner_name: str, repository_name: str) -> int:
        api_info_req_url = f'https://api.github.com/repos/{owner_name}/{repository_name}'
        return await self._get_nr_items_for_paginated_api_call(f'{api_info_req_url}/contributors')

    async def get_repository_nr_tags(self, owner_name: str, repository_name: str) -> int:
        api_info_req_url = f'https://api.github.com/repos/{owner_name}/{repository_name}'
        return await self._get_nr_items_for_paginated_api_call(f'{api_info_req_url}/tags')

    async def get_repository_default_branch(
            self, owner_name: str, repository_name: str, raise_on_failure: bool = True) -> Optional[str]:
        api_info_req_url = f'https://api.github.com/repos/{owner_name}/{repository_name}'
        repo_info_dict = await self._json_api_call(api_info_req_url, raise_on_failure=raise_on_failure)
        if repo_info_dict is None:
            assert not raise_on_failure
            return None
        return repo_info_dict['default_branch']

    async def get_repository_info(
            self, owner_name: str, repository_name: str,
            repo_dict: Optional[dict] = None) -> Optional[GithubRepositoryInfo]:
        api_info_req_url = f'https://api.github.com/repos/{owner_name}/{repository_name}'
        requests_coroutines = {
            # 'repo_main_language_freq': self.get_repo_main_language_freq(
            #     owner_name=owner_name, repository_name=repository_name, raise_on_failure=True),
            'repo_nr_contributors': self.get_repository_nr_contributors(
                owner_name=owner_name, repository_name=repository_name),
            'repo_nr_tags': self.get_repository_nr_tags(
                owner_name=owner_name, repository_name=repository_name),
            'repo_last_commits': self._paginated_list_api_call(f'{api_info_req_url}/commits', max_nr_items=20),
            'repo_nr_commits': self.get_repository_nr_commits(
                owner_name=owner_name, repository_name=repository_name),
            'repo_nr_branches': self.get_repository_nr_branches(
                owner_name=owner_name, repository_name=repository_name)
        }
        if repo_dict is None:
            requests_coroutines['repo_info_dict'] = self.get_repository_data(
                owner_name=owner_name, repository_name=repository_name)
        # invoke all together and wait for all to finish
        requests_tasks = {name: asyncio.create_task(coroutine) for name, coroutine in requests_coroutines.items()}
        await asyncio.gather(*requests_tasks.values())
        responses = {name: task.result() for name, task in requests_tasks.items()}
        repo_info_dict = responses['repo_info_dict'] if 'repo_info_dict' in responses else repo_dict
        # repo_main_language_freq = responses['repo_main_language_freq']
        repo_nr_contributors = responses['repo_nr_contributors']
        repo_nr_tags = responses['repo_nr_tags']
        repo_last_commits = responses['repo_last_commits']
        repo_nr_commits = responses['repo_nr_commits']
        repo_nr_branches = responses['repo_nr_branches']

        last_commits_datetimes = [
            datetime.datetime.strptime(commit['commit']['committer']['date'], "%Y-%m-%dT%H:%M:%SZ")
            for commit in repo_last_commits]  # 2021-09-23T05:14:39Z
        last_commits_dates_avg_datetime = datetime.datetime.fromtimestamp(
            sum(map(datetime.datetime.timestamp, last_commits_datetimes)) / len(last_commits_datetimes))
        last_commits_avg_time_delta = last_commits_dates_avg_datetime - datetime.datetime.today()
        if repo_info_dict is None:
            return None
        return GithubRepositoryInfo(
            nr_contributors=repo_nr_contributors,
            nr_stars=repo_info_dict['stargazers_count'],
            nr_commits=repo_nr_commits,
            nr_tags=repo_nr_tags,
            # nr_tags=None,
            nr_forks=repo_info_dict['forks_count'],
            nr_branches=repo_nr_branches,
            # nr_branches=None,
            nr_watchers=repo_info_dict['watchers_count'],
            network_count=None,  #repo_info_dict['network_count'],
            subscribers_count=None,  #repo_info_dict['subscribers_count'],
            # main_language_freq=repo_main_language_freq,
            last_commits_avg_time_delta=last_commits_avg_time_delta,
            default_branch=repo_info_dict['default_branch']
        )

    async def get_repo_main_language_freq(
            self, owner_name: str, repository_name: str, raise_on_failure: bool = True) -> Optional[float]:
        api_repo_languages_req_url = f'https://api.github.com/repos/{owner_name}/{repository_name}/languages'
        repo_languages_dict = await self._json_api_call(api_repo_languages_req_url, raise_on_failure=raise_on_failure)
        if repo_languages_dict is None:
            assert not raise_on_failure
            return None
        main_language = max(repo_languages_dict.keys(), default=None, key=repo_languages_dict.get)
        main_language_freq = \
            (repo_languages_dict[main_language] / sum(repo_languages_dict.values())) \
                if main_language in repo_languages_dict else 0.0
        return main_language_freq

    async def scrape_and_prepare_owners(
            self,
            owner_names: List[str],
            output_dir_path: str,
            popularity_check: bool = True,
            main_language: Optional[str] = None,
            min_main_language_freq: Optional[float] = None,
            file_extensions: Optional[List[str]] = None):
        tasks = []
        for owner_name in owner_names:
            tasks.append(asyncio.create_task(self.scrape_and_prepare_owner(
                owner_name=owner_name,
                output_dir_path=output_dir_path,
                popularity_check=popularity_check,
                main_language=main_language,
                min_main_language_freq=min_main_language_freq,
                file_extensions=file_extensions)))
            # Yield to avoid starving of sub-tasks. Prefer depth-first work-order scheduling over breadth-first.
            # Namely, allow download of the 1st project before finishing scraping data on all projects.
            await asyncio.sleep(1)
        await asyncio.gather(*tasks)

    async def get_repository_data(self, owner_name: str, repository_name: str) -> dict:
        api_repo_info_req_url = f'https://api.github.com/repos/{owner_name}/{repository_name}'
        repo_dict = await self._json_api_call(api_repo_info_req_url, raise_on_failure=True)
        return repo_dict

    async def iterate_repositories_data(self, owner_name: str, repository_names: List[str]) -> AsyncIterable[dict]:
        for repository_name in repository_names:
            repo_dict = await self.get_repository_data(owner_name=owner_name, repository_name=repository_name)
            if repo_dict is None:
                raise RuntimeError(f'Could not find repository {owner_name}/{repository_name}.')
            yield repo_dict

    async def scrape_and_prepare_owner(
            self, owner_name: str,
            output_dir_path: str,
            popularity_check: bool = True,
            repository_names: Optional[List[str]] = None,
            main_language: Optional[str] = None,
            min_main_language_freq: Optional[float] = None,
            file_extensions: Optional[List[str]] = None):
        tasks = []

        repositories_iterator = \
            self.find_all_repositories_of_owner(owner_name=owner_name) \
                if repository_names is None else \
                self.iterate_repositories_data(owner_name=owner_name, repository_names=repository_names)

        async for repository_dict in repositories_iterator:
            tasks.append(asyncio.create_task(self.scrape_and_prepare_repository_conditionally(
                owner_name=owner_name, repository_name=repository_dict['name'],
                output_dir_path=output_dir_path, repository_dict=repository_dict,
                file_extensions=file_extensions, main_language=main_language,
                min_main_language_freq=min_main_language_freq, popularity_check=popularity_check)))
            # Yield to avoid starving of sub-tasks. Prefer depth-first work-order scheduling over breadth-first.
            # Namely, allow download of the 1st project before finishing scraping data on all projects.
            await asyncio.sleep(1)
        await asyncio.gather(*tasks)

    async def scrape_and_prepare_repository_conditionally(
            self, owner_name: str, repository_name: str, output_dir_path: str,
            repository_dict: Optional[dict] = None, file_extensions: Optional[List[str]] = None,
            main_language: Optional[str] = None, min_main_language_freq: Optional[float] = None,
            popularity_check: bool = True):
        if main_language is not None and \
                (not repository_dict['language'] or
                 repository_dict['language'].lower() != main_language.lower()):
            return
        if min_main_language_freq is not None:
            main_language_freq = await self.get_repo_main_language_freq(
                owner_name=owner_name, repository_name=repository_dict['name'], raise_on_failure=True)
            if main_language_freq < min_main_language_freq - 2 * sys.float_info.epsilon:
                return
        if popularity_check:
            repository_info = await self.get_repository_info(
                owner_name=owner_name, repository_name=repository_name, repo_dict=repository_dict)
            if not is_repository_considered_popular(repo_info=repository_info):
                return
        await self.scrape_and_prepare_repository(
            owner_name=owner_name,
            repository_name=repository_name,
            branch_name=repository_dict['default_branch'],
            output_dir_path=output_dir_path,
            file_extensions=file_extensions)

    async def scrape_and_prepare_repository(
            self, owner_name: str, repository_name: str, output_dir_path: str,
            branch_name: Optional[str] = None, file_extensions: Optional[List[str]] = None):
        if branch_name is None:
            branch_name = await self.get_repository_default_branch(
                owner_name=owner_name, repository_name=repository_name)
        repository_long_name = f'{owner_name}/{repository_name}:{branch_name}'
        print(f'Cloning & preparing repo `{repository_long_name}` ..')
        for attempt_nr in range(1, self.max_nr_attempts + 1):
            if attempt_nr > 1:
                await asyncio.sleep(5 * (attempt_nr - 1))
            try:
                # We avoid making too many concurrent IOs to FS.
                async with self.concurrent_fs_io_processes_sem:
                    repo_output_dir = os.path.join(output_dir_path, repository_name)
                    repo_output_dir_path = aiopath.path.AsyncPath(repo_output_dir)
                    await repo_output_dir_path.mkdir(parents=True, exist_ok=True)
                    async with atempfile.TemporaryDirectory() as tmp_dir:
                        await self.clone_repository(
                            owner_name=owner_name,
                            repository_name=repository_name,
                            branch_name=branch_name,
                            target_zip_dir_path=tmp_dir,
                            target_unextracted_path=repo_output_dir if file_extensions is None else tmp_dir)
                        if file_extensions is not None:
                            await self.ioFileHelper.recursively_find_and_copy_all_files_with_certain_extension(
                                look_in_dir_path=tmp_dir, tgt_dir_path=repo_output_dir,
                                file_extensions=file_extensions)
                    break  # the operation succeeded - no further attempt needed
            except OSError as error:
                if attempt_nr == self.max_nr_attempts:
                    raise RuntimeError(f'Could not clone & prepare repository `{repository_long_name}` '
                                       f'due to an OS error [{error.errno}] (after {attempt_nr} attempts). '
                                       f'Error: {error}')
                else:
                    warn(f'OS Error [{error.errno}] occurred while trying to clone & prepare '
                         f'repository `{repository_long_name}` (after {attempt_nr}/{self.max_nr_attempts} attempts). '
                         f'Error: {error}')
            except asyncio.TimeoutError as error:
                if attempt_nr == self.max_nr_attempts:
                    raise RuntimeError(f'Could not clone & prepare repository `{repository_long_name}` '
                                       f'due to a timeout error (after {attempt_nr} attempts). '
                                       f'Error: {error}')
                else:
                    warn(f'Timeout error occurred while trying to clone & prepare '
                         f'repository `{repository_long_name}` (after {attempt_nr}/{self.max_nr_attempts} attempts). '
                         f'Error: {error}')

        print(f'Done cloning & preparing repo `{repository_long_name}`.')

    async def clone_repository(
            self, owner_name: str, repository_name: str, branch_name: str,
            target_zip_dir_path: str, target_unextracted_path: Optional[str] = None,
            chunk_size: int = 65535):
        repo_zip_file_url = f'https://github.com/{owner_name}/{repository_name}/archive/{branch_name}.zip'
        async with self.downloads_session_agent.request(repo_zip_file_url) as response:
            if response is None:
                raise RuntimeError(f'Could not download repository {owner_name}/{repository_name}:{branch_name}.')
            zip_file_path = os.path.join(target_zip_dir_path, f'{repository_name}.zip')
            async with aiofiles.open(zip_file_path, mode='wb') as zip_file:
                while True:
                    chunk = await response.content.read(chunk_size)
                    if not chunk:
                        break
                    await zip_file.write(chunk)
            # print('successful file download')
        if target_unextracted_path is not None:
            unzip_proc = await asyncio.create_subprocess_exec(
                'unzip', '-o', zip_file_path, '-d', target_unextracted_path,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL)
            await unzip_proc.communicate()
            exit_code = await unzip_proc.wait()
            assert exit_code == 0
        return

    async def close(self):
        await asyncio.gather(*(
            asyncio.create_task(self.api_session_agent.close()),
            asyncio.create_task(self.downloads_session_agent.close())))


async def _pre_solved_coroutine(result):
    return result


async def async_main():
    parser = create_argparser()
    args = parser.parse_args()
    scraper = GitHubScraper(
        user=args.user, token=args.token,
        max_nr_concurrent_api_requests=args.max_nr_concurrent_api_requests,
        max_concurrent_downloads=args.max_concurrent_downloads,
        max_nr_concurrent_fs_ios=args.max_nr_concurrent_fs_ios,
        max_nr_attempts=args.max_nr_attempts)
    try:
        if args.repository_names is None:
            await scraper.scrape_and_prepare_owners(
                owner_names=args.owner_names,
                output_dir_path=args.output_dir_path,
                popularity_check=not args.no_popularity_check,
                main_language=args.main_language,
                min_main_language_freq=args.min_main_language_freq,
                file_extensions=args.file_extensions)
        else:
            assert len(args.owner_names) == 1
            await scraper.scrape_and_prepare_owner(
                owner_name=args.owner_names[0],
                repository_names=args.repository_names,
                output_dir_path=args.output_dir_path,
                popularity_check=not args.no_popularity_check,
                main_language=args.main_language,
                min_main_language_freq=args.min_main_language_freq,
                file_extensions=args.file_extensions)
    finally:
        await scraper.close()


def sync_main():
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(async_main())


def create_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--owners', type=str, required=True, nargs='+', dest='owner_names')
    parser.add_argument('--repos', type=str, required=False, nargs='+', dest='repository_names')
    parser.add_argument('--no-popularity-check', action='store_true', dest='no_popularity_check')
    parser.add_argument('--output-dir', type=str, required=True, dest='output_dir_path')
    parser.add_argument('--user', type=str, required=False, dest='user')
    parser.add_argument('--token', type=str, required=False, dest='token')
    parser.add_argument('--main-language', type=str, required=False, dest='main_language')
    parser.add_argument('--main-language-freq', type=float, required=False, dest='min_main_language_freq')
    parser.add_argument('--extensions', type=str, required=False, dest='file_extensions', nargs='+')
    parser.add_argument('--concurrent-api-reqs', type=int, required=False,
                        dest='max_nr_concurrent_api_requests', default=5)
    parser.add_argument('--concurrent-downloads', type=int, required=False,
                        dest='max_concurrent_downloads', default=5)
    parser.add_argument('--concurrent-fs-ios', type=int, required=False,
                        dest='max_nr_concurrent_fs_ios', default=10)
    parser.add_argument('--attempts', type=int, required=False,
                        dest='max_nr_attempts', default=5)
    return parser


if __name__ == '__main__':
    sync_main()
