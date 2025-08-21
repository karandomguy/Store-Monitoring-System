from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration with environment variable support."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Database settings
    database_url: str = Field(
        default="postgresql://postgres:password@localhost:5432/store_monitoring",
        description="PostgreSQL database URL"
    )
    
    # Redis settings
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis URL for caching and Celery broker"
    )
    
    # Application settings
    app_name: str = "Store Monitoring API"
    app_version: str = "1.0.0"
    debug: bool = False
    
    # API settings
    api_prefix: str = "/api/v1"
    cors_origins: list[str] = ["*"]
    
    # Cache settings
    cache_ttl_seconds: int = 1800  # 30 minutes
    
    # Database connection pool settings
    db_pool_size: int = 10
    db_max_overflow: int = 20
    
    # Celery settings
    max_concurrent_reports: int = 5
    
    @property
    def async_database_url(self) -> str:
        # Convert psycopg2 URL to asyncpg URL
        return self.database_url.replace("postgresql://", "postgresql+asyncpg://")
    
    @property
    def celery_broker_url(self) -> str:
        # Get Celery broker URL
        return self.redis_url
    
    @property
    def celery_result_backend(self) -> str:
        # Get Celery result backend URL
        return self.redis_url


@lru_cache()
def get_settings() -> Settings:
    # Get cached application settings
    return Settings()

settings = get_settings()