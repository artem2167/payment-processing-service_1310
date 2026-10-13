import json
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Any, NoReturn

from fastapi import Request, Response
from fastapi.routing import APIRoute


def _reject_nonfinite_constant(token: str) -> NoReturn:
    raise json.JSONDecodeError(f"Non-standard JSON number: {token}", token, 0)


class DecimalAmountRequest(Request):
    async def json(self) -> Any:
        if not hasattr(self, "_json"):
            body = await self.body()
            parsed = json.loads(body, parse_constant=_reject_nonfinite_constant)
            if isinstance(parsed, dict) and isinstance(parsed.get("amount"), float):
                # Read money from its JSON token before binary float loses cents.
                # Other fields retain the normal JSON types, including metadata.
                exact = json.loads(
                    body, parse_float=Decimal, parse_constant=_reject_nonfinite_constant
                )
                parsed["amount"] = exact["amount"]
            self._json = parsed
        return self._json


class DecimalAmountRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Awaitable[Response]]:
        original_handler = super().get_route_handler()

        async def handler(request: Request) -> Response:
            return await original_handler(DecimalAmountRequest(request.scope, request.receive))

        return handler
