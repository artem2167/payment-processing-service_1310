import asyncio
import random

from app.config import Settings


class EmulatedGateway:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def __call__(self) -> str:
        await asyncio.sleep(
            random.uniform(self.settings.gateway_min_delay, self.settings.gateway_max_delay)
        )
        return "succeeded" if random.random() < 0.9 else "failed"
