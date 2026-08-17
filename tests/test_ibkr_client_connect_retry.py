from __future__ import annotations

from types import SimpleNamespace

import libs.infra.ibkr_client as ibkr_client


def test_build_ibkr_client_retries_timeout(monkeypatch) -> None:
    attempts: list[int] = []
    disconnected: list[int] = []

    class FakeClient:
        def __init__(self, settings) -> None:
            self.settings = settings
            self.index = len(attempts) + 1

        def connect_and_wait(self) -> None:
            attempts.append(self.index)
            if self.index < 3:
                raise TimeoutError(f"timeout-{self.index}")

        def disconnect_and_stop(self) -> None:
            disconnected.append(self.index)

    monkeypatch.setattr(ibkr_client, "IBClient", FakeClient)
    monkeypatch.setattr(ibkr_client._time, "sleep", lambda _seconds: None)

    client = ibkr_client.build_ibkr_client(SimpleNamespace())

    assert isinstance(client, FakeClient)
    assert attempts == [1, 2, 3]
    assert disconnected == [1, 2]


def test_build_ibkr_client_does_not_retry_non_timeout(monkeypatch) -> None:
    disconnected: list[int] = []

    class FakeClient:
        def __init__(self, settings) -> None:
            self.settings = settings
            self.index = 1

        def connect_and_wait(self) -> None:
            raise ConnectionError("boom")

        def disconnect_and_stop(self) -> None:
            disconnected.append(self.index)

    monkeypatch.setattr(ibkr_client, "IBClient", FakeClient)

    try:
        ibkr_client.build_ibkr_client(SimpleNamespace())
    except ConnectionError as exc:
        assert str(exc) == "boom"
    else:  # pragma: no cover - defensive
        raise AssertionError("expected ConnectionError")

    assert disconnected == [1]
