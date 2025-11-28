from pydantic_settings import BaseSettings



class Settings(BaseSettings):
    mongo_uri: str = "mongodb://localhost:27017/?replicaSet=rs0"
    mongo_db: str = "nyc_taxi"

    class Config:
        env_file = ".env"


settings = Settings()
