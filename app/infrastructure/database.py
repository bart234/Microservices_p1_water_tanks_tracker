
from sqlalchemy.orm import sessionmaker,Session
from app.db_cfg import engine


def get_db():
    Session=sessionmaker(bind=engine)    
    session=Session()
    yield session
    session.close()