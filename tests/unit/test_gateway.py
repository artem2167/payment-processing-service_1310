from unittest.mock import AsyncMock, Mock

import pytest

from app.services.gateway import EmulatedGateway


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "random_value,expected",
    [(0, "succeeded"), (0.899999, "succeeded"), (0.9, "failed"), (0.999, "failed")],
)
async def test_gateway_probability_boundary_and_nonblocking_delay(
    settings, monkeypatch, random_value, expected
):
    settings.gateway_min_delay = 2
    settings.gateway_max_delay = 5
    choose_delay = Mock(return_value=3.25)
    sleep = AsyncMock()
    monkeypatch.setattr("app.services.gateway.random.uniform", choose_delay)
    monkeypatch.setattr("app.services.gateway.random.random", lambda: random_value)
    monkeypatch.setattr("app.services.gateway.asyncio.sleep", sleep)
    assert await EmulatedGateway(settings)() == expected
    choose_delay.assert_called_once_with(2, 5)
    sleep.assert_awaited_once_with(3.25)
