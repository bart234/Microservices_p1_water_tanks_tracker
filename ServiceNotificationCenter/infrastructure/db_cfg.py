from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import os

load_dotenv()

def get_secrets_from_file(file_path, secret_from_sysenv):
    try:
        #if file_path added for  case when file_path will be none mean there is no set variable for file and will return none
        if file_path and os.path.exists(file_path):
            with open(f"{file_path}",'r') as file:
                return file.read().strip()
        else:
            return os.getenv(secret_from_sysenv)
    except IOError:
        return os.getenv(secret_from_sysenv)

DATABASE_URL="postgresql://{user}:{pwd}@{db_host}:5432/{db_name}".format(user=get_secrets_from_file(os.getenv("POSTGRES_USER_FILE"), "POSTGRES_USER"),
                                                                          pwd=get_secrets_from_file(os.getenv("POSTGRES_PASSWORD_FILE"), "POSTGRES_PASSWORD"),
                                                                          db_host=get_secrets_from_file(os.getenv("POSTGRES_DB_LOCALHOST_FILE"), "POSTGRES_DB_LOCALHOST"),
                                                                          db_name=get_secrets_from_file(os.getenv("POSTGRES_DB_FILE"), "POSTGRES_DB"))


engine = create_engine(DATABASE_URL,echo=False)

@contextmanager
def get_db():
    Session=sessionmaker(bind=engine)    
    session=Session()
    yield session
    session.close()
