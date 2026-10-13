from functools import lru_cache

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_key: str = Field(min_length=1)
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"
    retry_base_delay: float = Field(default=5, gt=0)
    webhook_timeout: float = Field(default=10, gt=0)
    outbox_poll_interval: float = Field(default=0.5, gt=0)
    publish_timeout: float = Field(default=5, gt=0)
    gateway_min_delay: float = Field(default=2, ge=0)
    gateway_max_delay: float = Field(default=5, ge=0)
    recovery_delay: float = Field(default=2, gt=0)
    log_level: str = "INFO"

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("API_KEY must not be blank")
        return value

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: str) -> str:
        if not value.startswith("postgresql+asyncpg://"):
            raise ValueError("DATABASE_URL must use postgresql+asyncpg")
        return value

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        normalized = value.upper()
        if normalized not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("Invalid log level")
        return normalized

    @model_validator(mode="after")
    def validate_delay_range(self) -> "Settings":
        if self.gateway_max_delay < self.gateway_min_delay:
            raise ValueError("GATEWAY_MAX_DELAY must be >= GATEWAY_MIN_DELAY")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
