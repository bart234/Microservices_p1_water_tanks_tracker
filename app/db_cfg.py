from dotenv import load_dotenv
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine
import os

load_dotenv()

def get_secrets(secret_name):
    try:
        with open(f"{secret_name}.txt",'r') as file:
            return file.read().strip()
    except IOError:
        return os.getenv(secret_name)

DATABASE_URL=f"postgresql://{get_secrets('db_user')}:{get_secrets('db_password')}@{get_secrets('db_localhost')}:5432/{get_secrets('db_postgres_db')}"

# DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./default.db")

engine = create_engine(DATABASE_URL,echo=True)

Base = declarative_base()
Base.metadata.create_all(engine)
