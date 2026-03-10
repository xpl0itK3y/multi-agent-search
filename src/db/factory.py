from sqlalchemy.orm import sessionmaker

from src.db.session import create_engine_from_settings


def create_session_factory():
    engine = create_engine_from_settings()
    return sessionmaker(bind=engine, autocommit=False, autoflush=False)
