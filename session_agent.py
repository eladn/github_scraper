import asyncio
import datetime
from contextlib import asynccontextmanager
from typing import Optional, Union

import aiohttp

from async_background_waiter import AsyncBackgroundWaiter


__all__ = ['SessionAgent']


class SessionAgent:
    def __init__(
            self,
            auth: Optional[aiohttp.BasicAuth] = None,
            max_nr_concurrent_requests: int = 5,
            max_nr_attempts: int = 5,
            reached_req_limit_http_response_status_code: int = 403):
        self.auth = auth
        self.reached_req_limit_http_response_status_code = reached_req_limit_http_response_status_code
        self.is_incoming_requests_blocked = False
        self.under_limit_wake_up_events = []
        self.concurrent_api_requests_sem = asyncio.BoundedSemaphore(max_nr_concurrent_requests)
        self.max_nr_attempts = max_nr_attempts
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_wakeup_time = datetime.datetime.now()
        self.nr_consecutive_failed_unblocking_attempts = -1
        self.nr_blocking_reasons_changes_during_consecutive_failed_unblocking_attempts = 0
        self.last_blocking_reason = None
        self.background_resources_closing_waiter = AsyncBackgroundWaiter()

    def create_session(self):
        # Note: only one concurrent coroutine can replace the session
        self.schedule_session_close_if_opened()
        if self.session is None:
            self.session = aiohttp.ClientSession(auth=self.auth)

    async def get_session(self) -> aiohttp.ClientSession:
        # Note: The `while` loop should be used if the create_session() was async and it could lead to a case where we
        #       would back-scheduled here only after the session does not exist anymore even we created it.
        # while self.session is None:
        #     self.create_session()
        if self.session is None:
            self.create_session()
        assert self.session is not None
        return self.session

    async def close(self):
        self.schedule_session_close_if_opened()
        await self.background_resources_closing_waiter.close()

    def schedule_session_close_if_opened(self):
        # Note: only one concurrent coroutine can enter the condition
        session = self.session
        if session is not None:
            self.session = None
            # Schedule the close without waiting for it. We don't want to block a request because of
            # waiting for a session to be closed.
            self.background_resources_closing_waiter.add_coroutine(session.close())

    async def _wait_if_requests_blocked(self):
        if not self.is_incoming_requests_blocked:
            return
        wake_up_events = asyncio.Event()
        self.under_limit_wake_up_events.append(wake_up_events)
        await wake_up_events.wait()

    async def _release_incoming_requests_blocking(self):
        assert self.is_incoming_requests_blocked
        max_wait_time = 60 * 2
        sleep_time = min(15 * 2 ** self.nr_consecutive_failed_unblocking_attempts, max_wait_time)
        print(f'Incoming requests will be unblocked within {sleep_time} seconds ..')
        await asyncio.sleep(sleep_time)
        assert self.is_incoming_requests_blocked
        print('Unblocking waiting and incoming requests.')
        # Note: No preemption can happen between unsetting the flag and emptying the list, as preemption
        #       is possible only if we yield (by an await). Otherwise, it would create a race where
        #       another coroutine could set the block flag again and another waiting coroutine would
        #       be added before we empty the list. Just remember that other async functions are not
        #       allowed to store a local pointer to this list and then yield, as the list might be
        #       replaced here. Indeed, the waiter only appends itself to this list.
        self.last_wakeup_time = datetime.datetime.now()
        self.is_incoming_requests_blocked = False
        under_limit_wake_up_events = self.under_limit_wake_up_events
        self.under_limit_wake_up_events = []
        for under_limit_wake_up_event in under_limit_wake_up_events:
            under_limit_wake_up_event.set()

    async def _block_all_incoming_requests(self, blocking_reason_kind, blocking_reason_msg: str) -> bool:
        if self.is_incoming_requests_blocked:
            return False
        # Note: only one async coroutine can pass this condition, as there is no preemption in this point.
        self.is_incoming_requests_blocked = True
        if self.last_blocking_reason != blocking_reason_kind and self.nr_consecutive_failed_unblocking_attempts >= 0:
            # If last blocking was due to another reason - reset the consecutive failed attempts counter; except for
            # the case when the blocking reason changed too many times during consecutive failed unblocking attempts.
            self.nr_blocking_reasons_changes_during_consecutive_failed_unblocking_attempts += 1
            if self.nr_blocking_reasons_changes_during_consecutive_failed_unblocking_attempts < 3:
                self.nr_consecutive_failed_unblocking_attempts = -1
        self.last_blocking_reason = blocking_reason_kind
        self.nr_consecutive_failed_unblocking_attempts += 1
        failed_wakeups_count_msg_str = \
            f' (after {self.nr_consecutive_failed_unblocking_attempts} failed wakeup attempts)' \
                if self.nr_consecutive_failed_unblocking_attempts > 0 else ''
        print(f'Temporarily blocking all incoming requests{failed_wakeups_count_msg_str}. '
              f'Blocking reason: {blocking_reason_msg}.')
        asyncio.create_task(self._release_incoming_requests_blocking())
        return True

    async def _block_all_incoming_requests_if_limit_reached(self, response_status: int) -> bool:
        if response_status != self.reached_req_limit_http_response_status_code:
            return False
        await self._block_all_incoming_requests(
            blocking_reason_kind='limit_reached', blocking_reason_msg='Limit reached')
        return True

    async def _block_all_incoming_requests_due_to_client_error(
            self, error: Union[aiohttp.ClientError, asyncio.TimeoutError]):
        am_i_the_blocker = await self._block_all_incoming_requests(
            blocking_reason_kind=error.__class__.__name__, blocking_reason_msg=f'Client error: {error}')
        if am_i_the_blocker:
            # Make the first requestor (after un-blocking) re-create the session.
            self.schedule_session_close_if_opened()

    @asynccontextmanager
    async def request(
            self, url, method='get', json: bool = False, body=None, headers=None, raise_on_failure: bool = True):
        request_additional_kwargs = {}
        if body is not None:
            request_additional_kwargs['body'] = body
        if headers is not None:
            request_additional_kwargs['headers'] = headers

        for attempt_nr in range(self.max_nr_attempts):
            if attempt_nr > 0:
                await asyncio.sleep(5 * attempt_nr)
            while True:
                    async with self.concurrent_api_requests_sem:
                        await self._wait_if_requests_blocked()
                        try:
                            session = await self.get_session()
                            last_wakeup_time_before_req = self.last_wakeup_time
                            async with session.request(method=method, url=url, **request_additional_kwargs) as response:
                                is_requests_limit_reached = \
                                    await self._block_all_incoming_requests_if_limit_reached(response.status)
                                if is_requests_limit_reached:
                                    continue  # inner loop - don't count as a failed attempt
                                if last_wakeup_time_before_req == self.last_wakeup_time:
                                    # no limit reached, hence we can reset the failed wakeup attempts count.
                                    self.nr_consecutive_failed_unblocking_attempts = -1
                                    self.nr_blocking_reasons_changes_during_consecutive_failed_unblocking_attempts = 0
                                if response.status != 200:
                                    break  # like "continue" for outer loop - counts as a failed attempt
                                if json:
                                    try:
                                        json = await response.json()
                                        yield response, json
                                        return
                                    except:
                                        break  # like "continue" for outer loop - counts as a failed attempt
                                else:
                                    yield response
                                    return
                        except (aiohttp.ClientError, asyncio.TimeoutError) as error:
                            await self._block_all_incoming_requests_due_to_client_error(error)
                            continue  # inner loop - don't count as a failed attempt
                        except asyncio.CancelledError as error:
                            # The used session might have been closed by another failed request that triggered
                            # blocking. In such case we might reach here.
                            continue  # inner loop - don't count as a failed attempt

        if raise_on_failure:
            raise RuntimeError(f'Maximum attempts reached for request `{url}`.')
        yield None
        return
