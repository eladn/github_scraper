import sys
import datetime
import dataclasses
from typing import Iterable

from github_repository_info import GithubRepositoryInfo


__all__ = ['is_repository_considered_popular']


def is_repository_considered_popular(repo_info: GithubRepositoryInfo) -> bool:
    repo_info_relaxed_conditions = GithubRepositoryInfo(
        nr_contributors=5,
        nr_stars=40,
        nr_commits=3000,
        nr_tags=5,
        nr_forks=10,
        nr_branches=5,
        nr_watchers=10,
        network_count=None,
        subscribers_count=None,
        last_commits_avg_time_delta=datetime.timedelta(days=-4 * 30),
    )
    repo_info_moderate_conditions = GithubRepositoryInfo(
        nr_contributors=20,
        nr_stars=75,
        nr_commits=4500,
        nr_tags=15,
        nr_forks=40,
        nr_branches=15,
        nr_watchers=50,
        network_count=None,
        subscribers_count=None,
        last_commits_avg_time_delta=datetime.timedelta(days=-1.5 * 30),
    )
    repo_info_strict_conditions = GithubRepositoryInfo(
        nr_contributors=80,
        nr_stars=2_000,
        nr_commits=7_000,
        nr_tags=80,
        nr_forks=100,
        nr_branches=30,
        nr_watchers=200,
        network_count=None,
        subscribers_count=None,
        last_commits_avg_time_delta=datetime.timedelta(days=-5),
    )

    def check_conditions(conditions: GithubRepositoryInfo) -> Iterable[bool]:
        return (
            getattr(repo_info, field.name) >= getattr(conditions, field.name)
            for field in dataclasses.fields(conditions)
            if getattr(repo_info, field.name) is not None and
            getattr(conditions, field.name) is not None and
            isinstance(getattr(conditions, field.name), (int, float, datetime.timedelta)))

    def mean(data):
        nr_items, mean_accumulator = 0, 0.0
        for x in data:
            nr_items += 1
            mean_accumulator += (x - mean_accumulator) / nr_items
        return float('nan') if nr_items < 1 else mean_accumulator

    relaxed_conjunction = mean(check_conditions(repo_info_relaxed_conditions)) > 0.75 - 2 * sys.float_info.epsilon
    moderate_majority = mean(check_conditions(repo_info_moderate_conditions)) > 0.5 - 2 * sys.float_info.epsilon
    strict_disjunction = mean(check_conditions(repo_info_strict_conditions)) > 0.2 - 2 * sys.float_info.epsilon
    is_repo_popular = \
        mean((relaxed_conjunction, strict_disjunction, moderate_majority)) > 0.5 - sys.float_info.epsilon
    return is_repo_popular
