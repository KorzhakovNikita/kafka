from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SERVER_HOST: str
    SERVER_PORT: int

    # kafka
    KAFKA_BROKER: str
    KAFKA_API_VERSION: str
    TOPIC_NAME: str

    # email
    SMTP_HOST: str
    SMTP_PORT: int
    SMTP_USERNAME: str
    SMTP_PASSWORD: str

    BASE_DIR: Path = Path(__file__).parent


settings = Settings(_env_file=".env", _env_file_encoding="utf-8")