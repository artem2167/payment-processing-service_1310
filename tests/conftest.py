"""Explicit service opt-in and isolated schemas keep tests away from app data."""

import os
from collections.abc import AsyncIterator
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import Settings
from app.db import Database
from app.main import create_app
from app.models import Base


@pytest.fixture
def settings() -> Settings:
    return Settings(
        api_key="test-secret",
        retry_base_delay=5,
        gateway_min_delay=0,
        gateway_max_delay=0,
        recovery_delay=0.01,
    )


@pytest_asyncio.fixture
async def database() -> AsyncIterator[Database]:
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set TEST_DATABASE_URL to an explicitly designated PostgreSQL test database")
    if not url.startswith("postgresql+asyncpg://"):
        pytest.fail(
            "TEST_DATABASE_URL must use PostgreSQL with asyncpg; "
            "SQLite does not test these guarantees"
        )

    schema = f"payments_test_{uuid4().hex}"
    admin_engine = create_async_engine(url)
    database = Database(url)
    await database.engine.dispose()
    database.engine = create_async_engine(
        url,
        connect_args={"server_settings": {"search_path": schema}},
    )
    database.sessions = async_sessionmaker(database.engine, expire_on_commit=False)
    try:
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with database.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        yield database
    finally:
        await database.close()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin_engine.dispose()


@pytest_asyncio.fixture
async def client(database: Database, settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings=settings, database=database)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://payments.test",
        headers={"X-API-Key": settings.api_key},
    ) as client:
        yield client


@pytest.fixture
def payment_body() -> dict:
    return {
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": "42", "nested": {"items": [1, 2]}},
        "webhook_url": "http://receiver.test/events",
    }
