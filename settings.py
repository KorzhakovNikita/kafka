from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SMTP_HOST: str
    SMTP_PORT: int
    SMTP_USERNAME: str
    SMTP_PASSWORD: str
    BASE_DIR: Path = Path(__file__).parent


settings = Settings(_env_file=".env", _env_file_encoding="utf-8")