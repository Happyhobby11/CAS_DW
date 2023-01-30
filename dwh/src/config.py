import os
from dotenv import load_dotenv

load_dotenv()
from dataclasses import dataclass


@dataclass
class config:
    SPARK_MASTER: str = os.getenv("SPARK_MASTER")
    POSTGRES_DB_APP: str = os.getenv("POSTGRES_DB_APP")
    POSTGRES_USER_APP: str = os.getenv("POSTGRES_USER_APP")
    POSTGRES_PASSWORD_APP: str = os.getenv("POSTGRES_PASSWORD_APP")
    POSTGRES_HOST_APP: str = os.getenv("POSTGRES_HOST_APP")
    POSTGRES_DB_DW: str = os.getenv("POSTGRES_DB_DW")
    POSTGRES_USER_DW: str = os.getenv("POSTGRES_USER_DW")
    POSTGRES_PASSWORD_DW: str = os.getenv("POSTGRES_PASSWORD_DW")
    POSTGRES_HOST_DW: str = os.getenv("POSTGRES_HOST_DW")


cfg = config()
