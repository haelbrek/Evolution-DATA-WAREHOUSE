from pathlib import Path
from typing import List, Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


API_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(API_DIR / ".env"),
        env_ignore_empty=True,
    )

    azure_sql_server: str
    azure_sql_database: str
    azure_sql_username: str
    azure_sql_password: str
    azure_sql_schema: str = "dbo"
    azure_sql_driver: str = "ODBC Driver 18 for SQL Server"
    azure_sql_port: int = 1433
    azure_sql_chunksize: int = 100
    allowed_tables: Optional[List[str]] = None

    @field_validator("allowed_tables", mode="before")
    @classmethod
    def _split_tables(cls, value: object) -> Optional[List[str]]:
        if value is None:
            return None
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        if isinstance(value, (list, tuple)):
            return [str(item).strip() for item in value if str(item).strip()]
        return None

    @property
    def sqlalchemy_dsn(self) -> str:
        driver_token = self.azure_sql_driver.replace(" ", "+")
        return (
            f"mssql+pyodbc://{self.azure_sql_username}:{self.azure_sql_password}"
            f"@{self.azure_sql_server}:{self.azure_sql_port}/{self.azure_sql_database}?driver={driver_token}"
        )

    @property
    def allowed_tables_set(self) -> Optional[set[str]]:
        if not self.allowed_tables:
            return None
        return set(self.allowed_tables)


settings = Settings()  # type: ignore[call-arg]
