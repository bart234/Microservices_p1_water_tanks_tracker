from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine
from BackendCore.settings.db_setting import DATABASE_URL


if "None" in DATABASE_URL:
    print(f"---DB conn_str: {DATABASE_URL}")    
    raise "Log ERROR: DB connection strings  contain None fields. Credentials not loaded correctly."

# DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./default.db")
try:
    engine = create_engine(DATABASE_URL,echo=True)
except Exception as e:
    raise RuntimeError(f"Log ERROR: create_engine failed, check DB conenction string.ERROR: {e}")
    
Base = declarative_base()
