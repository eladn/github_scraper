import datetime
import dataclasses
from typing import Optional


__all__ = ['GithubRepositoryInfo']


@dataclasses.dataclass(frozen=True)
class GithubRepositoryInfo:
    nr_contributors: Optional[int] = None
    nr_stars: Optional[int] = None
    nr_commits: Optional[int] = None
    nr_tags: Optional[int] = None
    nr_forks: Optional[int] = None
    nr_branches: Optional[int] = None
    nr_watchers: Optional[int] = None
    network_count: Optional[int] = None
    subscribers_count: Optional[int] = None
    main_language_freq: Optional[float] = None
    last_commits_avg_time_delta: Optional[datetime.timedelta] = None
    default_branch: Optional[str] = None
