from pydantic import field_validator
from pydantic_core.core_schema import ValidationInfo
from pydantic_settings import BaseSettings, SettingsConfigDict


class ElasticLoggerConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')
    elastic_port: str
    elastic_host: str
    elastic_username: str
    elastic_password: str
    elastic_url: str | None = None
    elastic_log_index_name: str
    elastic_log_level: str = "WARNING"

    @field_validator("elastic_url")
    @classmethod
    def validate_elastic_url(cls, value: str | None, values: ValidationInfo):
        if value is None:
            return f"{values.data['elastic_host']}:{values.data['elastic_port']}"
        return value

