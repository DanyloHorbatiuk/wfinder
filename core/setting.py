from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent

class Settings(BaseSettings):
    minio_root_user: str
    minio_root_password: str
    minio_bucket_name: str
    minio_endpoint: str

    postgres_user: str
    postgres_password: str
    postgres_url: str

    telegram_token: str
    telegram_chat_id: str

    notify_dry_run: bool = True

    guard_min_ratio: float = 0.5
    guard_history_n: int = 7
    guard_min_history: int = 3
    guard_max_close_share: float = 0.5
    guard_min_active: int = 10

    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()