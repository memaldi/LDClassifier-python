from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime

Base = declarative_base()

class Task(Base):
    __tablename__ = 'task'

    id = Column(Integer, primary_key=True)
    endpoint = Column(String)
    graph = Column(String)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    paused_since = Column(DateTime)
    offset = Column(Integer)

class Settings(Base):
    __tablename__ = 'settings'

    id = Column(Integer, primary_key=True)
    virtuoso_endpoint = Column(String)
