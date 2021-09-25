import asyncio
from typing import Optional


all = ['AsyncBackgroundWaiter']


class AsyncBackgroundWaiter:
    def __init__(self):
        self._next_waiter_id = 0
        self._wait_for = {}
        self._finished_waiters_ids_that_should_be_deleted = set()
        self._finished_sem = asyncio.Semaphore(0)
        self._background_finished_collector_task: Optional[asyncio.Task] = None

    def add_coroutine(self, coroutine):
        waiter_id = self._next_waiter_id
        self._next_waiter_id += 1
        assert waiter_id not in self._wait_for
        self._wait_for[waiter_id] = asyncio.create_task(self._single_task_waiter(coroutine, waiter_id))
        if self._background_finished_collector_task is None:
            self._background_finished_collector_task = asyncio.create_task(
                self._background_finished_waiters_tasks_collector())

    async def close(self):
        if self._background_finished_collector_task is None or self._background_finished_collector_task.done():
            return
        assert not self._background_finished_collector_task.cancelled()
        await self._background_finished_collector_task
        assert len(self._wait_for) == 0

    async def _single_task_waiter(self, coroutine, waiter_id: int):
        await coroutine
        self._finished_waiters_ids_that_should_be_deleted.add(waiter_id)
        # signal the `_background_finished_waiters_tasks_collector` this task should be collected
        self._finished_sem.release()

    async def _background_finished_waiters_tasks_collector(self):
        # The `_single_task_waiter`s cannot remove themselves from the wait list. Instead, they signal that they
        # are done and we remove them here so the `wait_for` collection won't get huge over time.
        while len(self._wait_for):
            await self._finished_sem.acquire()
            assert len(self._finished_waiters_ids_that_should_be_deleted) > 0
            waiter_id = self._finished_waiters_ids_that_should_be_deleted.pop()
            assert waiter_id in self._wait_for
            waiter_task = self._wait_for.pop(waiter_id)
            if not waiter_task.done():
                await waiter_task
