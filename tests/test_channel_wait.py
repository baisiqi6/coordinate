"""Waits retain caller cursors, release capacity on cancellation and have deadlines."""
from __future__ import annotations

import asyncio

import pytest

from test_channel_read import setup, principal, cursor, message


class Clock:
    def __init__(self):
        self.now = 0.0
        self.hook = None
        self.sleeps = []

    def __call__(self):
        return self.now

    async def sleep(self, delay):
        self.sleeps.append(delay)
        self.now += delay
        if self.hook:
            self.hook()


def run(service, **overrides):
    request = dict(workspace_id="ws-a", channel_id="123", after_cursor=cursor(), timeout_seconds=6)
    request.update(overrides)
    return asyncio.run(service.wait(request, principal()))


def clock_service(service):
    clock = Clock()
    service._clock, service._sleep = clock, clock.sleep
    return clock


def test_wait_returns_immediately_when_messages_exist(setup):
    service, fake, _ = setup
    fake.messages = [message(1)]
    clock = clock_service(service)
    result = run(service)
    assert result["ok"]
    assert result["data"]["status"] == "messages"
    assert result["data"]["messages"][0]["message_id"] == "1"
    assert clock.sleeps == []


def test_wait_timeout_preserves_cursor_with_bounded_polling(setup):
    service, _, _ = setup
    clock = clock_service(service)
    result = run(service)
    assert result["data"]["status"] == "timeout"
    assert result["data"]["next_cursor"] == cursor()
    assert result["data"]["messages"] == []
    assert clock.now == 6
    assert all(delay <= 2 for delay in clock.sleeps)


def test_zero_timeout_still_checks_once(setup):
    service, fake, _ = setup
    clock = clock_service(service)
    assert run(service, timeout_seconds=0)["data"]["status"] == "timeout"
    assert clock.sleeps == []
    assert sum(path.startswith("/channels/123/messages?") for path, _ in fake.calls) == 1


def test_arrival_during_wait_returns_next_page(setup):
    service, fake, _ = setup
    clock = clock_service(service)
    clock.hook = lambda: fake.messages.append(message(1))
    result = run(service)
    assert result["data"]["status"] == "messages"
    assert clock.now == 2
    assert result["data"]["next_cursor"] != cursor()


def test_binding_release_during_wait_is_forbidden(setup):
    service, _, conn = setup
    clock = clock_service(service)
    def release():
        conn.execute("DELETE FROM channel_bindings")
        conn.commit()
    clock.hook = release
    assert run(service)["error"]["code"] == "forbidden"


@pytest.mark.parametrize("code", ["forbidden", "rate_limited", "upstream_unavailable"])
def test_upstream_failures_do_not_become_timeout(setup, code):
    from coordinate.discord_history import DiscordHistoryError
    service, fake, _ = setup
    clock = clock_service(service)
    def reject(path):
        raise DiscordHistoryError(code, "Static failure.")
    clock.hook = lambda: setattr(fake, "fail", reject)
    result = run(service)
    assert result["error"]["code"] == code
    assert result["data"] is None


def test_permission_revocation_during_wait_is_forbidden(setup):
    service, fake, _ = setup
    clock = clock_service(service)
    clock.hook = lambda: setattr(fake, "permissions", 0)
    assert run(service)["error"]["code"] == "forbidden"


@pytest.mark.parametrize("overrides", [dict(after_cursor=None), dict(after_cursor="bad"), dict(timeout_seconds=-1), dict(timeout_seconds=31), dict(timeout_seconds=True)])
def test_invalid_wait_input_is_rejected(setup, overrides):
    service, fake, _ = setup
    result = run(service, **overrides)
    assert not result["ok"]
    assert fake.calls == []


def test_wait_requires_its_own_exact_grant(setup):
    service, fake, _ = setup
    async def scenario():
        return await service.wait(dict(workspace_id="ws-a", channel_id="123", after_cursor=cursor()), principal(tools=frozenset({"coordinate.channel_read"})))
    assert asyncio.run(scenario())["error"]["code"] == "forbidden"
    assert fake.calls == []


def test_cancellation_releases_principal_capacity(setup):
    service, _, _ = setup
    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        async def sleep(delay):
            entered.set()
            await release.wait()
        service._sleep = sleep
        request = dict(workspace_id="ws-a", channel_id="123", after_cursor=cursor(), timeout_seconds=30)
        first = asyncio.create_task(service.wait(request, principal()))
        await entered.wait()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        result = await service.wait({**request, "timeout_seconds": 0}, principal())
        assert result["data"]["status"] == "timeout"
    asyncio.run(scenario())


def test_per_principal_wait_limit_is_two_and_does_not_queue(setup):
    service, _, _ = setup
    async def scenario():
        entered = asyncio.Queue()
        release = asyncio.Event()
        async def sleep(delay):
            await entered.put(True)
            await release.wait()
        service._sleep = sleep
        request = dict(workspace_id="ws-a", channel_id="123", after_cursor=cursor(), timeout_seconds=30)
        tasks = [asyncio.create_task(service.wait(request, principal())) for _ in range(2)]
        try:
            await entered.get()
            await entered.get()
            result = await service.wait(request, principal())
            assert result["error"]["code"] == "busy"
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    asyncio.run(scenario())


def test_process_wait_limit_is_eight_across_principals(setup):
    service, _, _ = setup
    async def scenario():
        entered = asyncio.Queue()
        release = asyncio.Event()
        async def sleep(delay):
            await entered.put(True)
            await release.wait()
        service._sleep = sleep
        request = dict(workspace_id="ws-a", channel_id="123", after_cursor=cursor(), timeout_seconds=30)
        tasks = [asyncio.create_task(service.wait(request, principal(client_id=f"client-{n // 2}"))) for n in range(8)]
        try:
            for _ in tasks:
                await entered.get()
            result = await service.wait(request, principal(client_id="ninth"))
            assert result["error"]["code"] == "busy"
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    asyncio.run(scenario())


def test_slow_upstream_exhausting_deadline_is_error_not_timeout(setup):
    service, fake, _ = setup
    clock = clock_service(service)
    def slow(path):
        clock.now += 1
    fake.fail = slow
    result = run(service, timeout_seconds=1)
    assert result["error"]["code"] == "upstream_unavailable"
    assert result["data"] is None


def test_binding_connection_is_closed_before_io_and_sleep(setup):
    service, fake, _ = setup
    clock = clock_service(service)
    factory = service._connection_factory
    opened = [0]
    class Connection:
        def __init__(self):
            self.connection = factory()
            opened[0] += 1
        def __getattr__(self, name):
            return getattr(self.connection, name)
        def close(self):
            self.connection.close()
            opened[0] -= 1
    service._connection_factory = Connection
    def check(path):
        assert opened[0] == 0
    fake.fail = check
    clock.hook = lambda: check(None)
    assert run(service)["data"]["status"] == "timeout"
    assert opened[0] == 0


def test_deadline_exhausted_before_first_platform_read_is_upstream_error(setup):
    service, fake, _ = setup
    clock = clock_service(service)
    factory = service._connection_factory
    calls = [0]
    def expire():
        calls[0] += 1
        if calls[0] == 1:
            clock.now = 2
        return factory()
    service._connection_factory = expire
    result = run(service, timeout_seconds=1)
    assert result["error"]["code"] == "upstream_unavailable"
    assert fake.calls == []


def test_blocked_authorization_does_not_block_event_loop_or_extend_deadline(setup):
    import threading
    import time
    service, fake, _ = setup
    release = threading.Event()
    original = service._authorize
    def blocked(request, who, operation):
        release.wait(3)
        original(request, who, operation)
    service._authorize = blocked
    async def scenario():
        ticked = []
        async def ticker():
            await asyncio.sleep(0.05)
            ticked.append(True)
        tick = asyncio.create_task(ticker())
        started = time.monotonic()
        try:
            result = await service.wait(dict(workspace_id="ws-a", channel_id="123", after_cursor=cursor(), timeout_seconds=1), principal())
            elapsed = time.monotonic() - started
            assert result["error"]["code"] == "upstream_unavailable"
            assert 0.9 <= elapsed < 1.5
            assert ticked
            assert fake.calls == []
        finally:
            release.set()
            await tick
    asyncio.run(scenario())


def test_wait_limits_include_in_progress_authorization(setup):
    import threading
    service, _, _ = setup
    release = threading.Event()
    original = service._authorize
    async def scenario():
        loop = asyncio.get_running_loop()
        entered = asyncio.Queue()
        def blocked(request, who, operation):
            loop.call_soon_threadsafe(entered.put_nowait, True)
            release.wait(3)
            original(request, who, operation)
        service._authorize = blocked
        request = dict(workspace_id="ws-a", channel_id="123", after_cursor=cursor(), timeout_seconds=1)
        tasks = [asyncio.create_task(service.wait(request, principal())) for _ in range(2)]
        try:
            await entered.get()
            await entered.get()
            result = await service.wait(request, principal())
            assert result["error"]["code"] == "busy"
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
    asyncio.run(scenario())


def timed_platform(service, fake, clock):
    history = service._history()
    history._clock = clock
    def request(method, path, *, timeout):
        response = fake(method, path, timeout=timeout)
        clock.now += 0.5
        return response
    history._request_json = request


def test_wait_can_return_messages_after_a_healthy_page_longer_than_five_seconds(setup):
    service, fake, _ = setup
    clock = clock_service(service)
    timed_platform(service, fake, clock)
    fake.messages = [message(1)]
    result = run(service, timeout_seconds=20)
    assert result["ok"]
    assert result["data"]["status"] == "messages"
    assert result["data"]["messages"][0]["message_id"] == "1"
    assert clock.now == 6.5
    assert all(0 < timeout <= 5 for _, timeout in fake.calls)


def test_zero_wait_allows_one_healthy_page_longer_than_five_seconds(setup):
    service, fake, _ = setup
    clock = clock_service(service)
    timed_platform(service, fake, clock)
    result = run(service, timeout_seconds=0)
    assert result["ok"]
    assert result["data"]["status"] == "timeout"
    assert result["data"]["next_cursor"] == cursor()
    assert clock.now == 6.5
    assert clock.sleeps == []


def test_local_deadline_during_later_healthy_empty_poll_is_normal_timeout(setup):
    service, fake, _ = setup
    clock = clock_service(service)
    timed_platform(service, fake, clock)
    result = run(service, timeout_seconds=12)
    assert result["ok"]
    assert result["data"]["status"] == "timeout"
    assert result["data"]["next_cursor"] == cursor()
    assert result["data"]["messages"] == []
    assert clock.now == 12
    assert sum(path.startswith("/channels/123/messages?") for path, _ in fake.calls) >= 1


@pytest.mark.parametrize("failure", ["forbidden", "rate_limited", "network"])
def test_real_failure_at_deadline_after_successful_empty_poll_remains_error(setup, failure):
    from coordinate.discord_history import DiscordHistoryError
    service, fake, _ = setup
    clock = clock_service(service)
    history = service._history()
    history._clock = clock
    fail_now = [False]
    def request(method, path, *, timeout):
        response = fake(method, path, timeout=timeout)
        if fail_now[0]:
            clock.now = 12.5
            if failure == "network":
                raise OSError("isolated connection failure")
            raise DiscordHistoryError(failure, "Static platform failure.")
        clock.now += 0.5
        return response
    history._request_json = request
    clock.hook = lambda: fail_now.__setitem__(0, True)
    result = run(service, timeout_seconds=12)
    assert not result["ok"]
    assert result["error"]["code"] == ("upstream_unavailable" if failure == "network" else failure)
    assert result["data"] is None


@pytest.mark.parametrize("prior_healthy", [False, True])
def test_async_page_deadline_is_timeout_only_after_a_successful_empty_page(setup, prior_healthy):
    import threading
    service, fake, _ = setup
    release = threading.Event()
    history = service._history()
    original = history.read_page
    calls = [0]
    def blocking_page(*args, **kwargs):
        calls[0] += 1
        if prior_healthy and calls[0] == 1:
            return original(*args, **kwargs)
        release.wait(3)
        return {"messages": [], "has_more": False}
    async def immediate_sleep(delay):
        return None
    history.read_page = blocking_page
    service._sleep = immediate_sleep
    async def scenario():
        try:
            result = await service.wait(dict(workspace_id="ws-a", channel_id="123", after_cursor=cursor(), timeout_seconds=1), principal())
            if prior_healthy:
                assert result["ok"]
                assert result["data"]["status"] == "timeout"
                assert result["data"]["next_cursor"] == cursor()
            else:
                assert result["error"]["code"] == "upstream_unavailable"
                assert result["data"] is None
        finally:
            release.set()
    asyncio.run(scenario())
    assert calls[0] == (2 if prior_healthy else 1)
