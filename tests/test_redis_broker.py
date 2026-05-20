from unittest.mock import MagicMock, patch

import pytest

from src.brokers.redis_broker import (
    FINALIZE_JOBS_QUEUE,
    SEARCH_JOBS_QUEUE,
    RedisBroker,
)


@pytest.fixture
def broker():
    with patch("src.brokers.redis_broker.redis.from_url"):
        return RedisBroker("redis://localhost:6379/0", pop_timeout_seconds=1)


def _mock_client(broker: RedisBroker) -> MagicMock:
    client = MagicMock()
    broker._client = client
    return client


# ---------------------------------------------------------------------------
# Search jobs — push
# ---------------------------------------------------------------------------


def test_push_search_job_calls_rpush(broker):
    client = _mock_client(broker)
    broker.push_search_job("job-1")
    client.rpush.assert_called_once_with(SEARCH_JOBS_QUEUE, "job-1")


def test_push_search_job_swallows_redis_error(broker):
    client = _mock_client(broker)
    client.rpush.side_effect = Exception("connection refused")
    broker.push_search_job("job-1")  # не должен бросать исключение


# ---------------------------------------------------------------------------
# Search jobs — pop
# ---------------------------------------------------------------------------


def test_pop_search_job_returns_job_id(broker):
    client = _mock_client(broker)
    client.blpop.return_value = (SEARCH_JOBS_QUEUE, "job-1")

    result = broker.pop_search_job()

    assert result == "job-1"
    client.blpop.assert_called_once_with(SEARCH_JOBS_QUEUE, timeout=1)


def test_pop_search_job_returns_none_on_timeout(broker):
    client = _mock_client(broker)
    client.blpop.return_value = None

    assert broker.pop_search_job() is None


def test_pop_search_job_returns_none_on_redis_error(broker):
    client = _mock_client(broker)
    client.blpop.side_effect = Exception("connection lost")

    assert broker.pop_search_job() is None


# ---------------------------------------------------------------------------
# Finalize jobs — push
# ---------------------------------------------------------------------------


def test_push_finalize_job_calls_rpush(broker):
    client = _mock_client(broker)
    broker.push_finalize_job("job-2")
    client.rpush.assert_called_once_with(FINALIZE_JOBS_QUEUE, "job-2")


def test_push_finalize_job_swallows_redis_error(broker):
    client = _mock_client(broker)
    client.rpush.side_effect = Exception("connection refused")
    broker.push_finalize_job("job-2")  # не должен бросать исключение


# ---------------------------------------------------------------------------
# Finalize jobs — pop
# ---------------------------------------------------------------------------


def test_pop_finalize_job_returns_job_id(broker):
    client = _mock_client(broker)
    client.blpop.return_value = (FINALIZE_JOBS_QUEUE, "job-2")

    result = broker.pop_finalize_job()

    assert result == "job-2"
    client.blpop.assert_called_once_with(FINALIZE_JOBS_QUEUE, timeout=1)


def test_pop_finalize_job_returns_none_on_timeout(broker):
    client = _mock_client(broker)
    client.blpop.return_value = None

    assert broker.pop_finalize_job() is None


def test_pop_finalize_job_returns_none_on_redis_error(broker):
    client = _mock_client(broker)
    client.blpop.side_effect = Exception("connection lost")

    assert broker.pop_finalize_job() is None


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def test_ping_returns_true(broker):
    client = _mock_client(broker)
    client.ping.return_value = True

    assert broker.ping() is True


def test_ping_returns_false_on_redis_error(broker):
    client = _mock_client(broker)
    client.ping.side_effect = Exception("unreachable")

    assert broker.ping() is False


def test_close_calls_client_close(broker):
    client = _mock_client(broker)
    broker.close()
    client.close.assert_called_once()


def test_close_swallows_error(broker):
    client = _mock_client(broker)
    client.close.side_effect = Exception("already closed")
    broker.close()  # не должен бросать исключение


# ---------------------------------------------------------------------------
# _create_broker bootstrap helper
# ---------------------------------------------------------------------------


def test_create_broker_returns_none_when_disabled(mocker):
    from src.bootstrap import _create_broker

    mocker.patch("src.bootstrap.settings", use_redis_broker=False, redis_url=None)

    assert _create_broker() is None


def test_create_broker_returns_none_when_no_url(mocker):
    from src.bootstrap import _create_broker

    mocker.patch("src.bootstrap.settings", use_redis_broker=True, redis_url=None)

    assert _create_broker() is None


def test_create_broker_returns_broker_when_ping_ok(mocker):
    from src.bootstrap import _create_broker

    mocker.patch(
        "src.bootstrap.settings",
        use_redis_broker=True,
        redis_url="redis://localhost:6379/0",
        redis_broker_pop_timeout_seconds=2,
    )
    mock_broker = MagicMock()
    mock_broker.ping.return_value = True
    mocker.patch("src.bootstrap.RedisBroker", return_value=mock_broker)

    result = _create_broker()

    assert result is mock_broker


def test_create_broker_returns_none_when_ping_fails(mocker):
    from src.bootstrap import _create_broker

    mocker.patch(
        "src.bootstrap.settings",
        use_redis_broker=True,
        redis_url="redis://localhost:6379/0",
        redis_broker_pop_timeout_seconds=2,
    )
    mock_broker = MagicMock()
    mock_broker.ping.return_value = False
    mocker.patch("src.bootstrap.RedisBroker", return_value=mock_broker)

    assert _create_broker() is None


def test_create_broker_returns_none_on_exception(mocker):
    from src.bootstrap import _create_broker

    mocker.patch(
        "src.bootstrap.settings",
        use_redis_broker=True,
        redis_url="redis://localhost:6379/0",
        redis_broker_pop_timeout_seconds=2,
    )
    mocker.patch("src.bootstrap.RedisBroker", side_effect=Exception("cannot connect"))

    assert _create_broker() is None
