
from sqlalchemy.orm import sessionmaker,Session
from Backend_core.db_cfg import engine


def get_db():
    Session=sessionmaker(bind=engine)    
    session=Session()
    yield session
    session.close()