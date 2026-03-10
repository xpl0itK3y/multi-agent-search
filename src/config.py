from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    deepseek_api_key: Optional[str] = None
    deepseek_model: str = "deepseek-chat"
    
    app_name: str = "Prompt Optimizer API"
    debug: bool = False
    
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
