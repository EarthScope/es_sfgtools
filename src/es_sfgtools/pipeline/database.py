import datetime
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    ForeignKey,
    Text,
    JSON,
    Enum

)
from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import Mapped, declarative_base, relationship,mapped_column
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()

class Assets(Base):
    __tablename__ = "assets"
    id = Column(Integer,primary_key=True,autoincrement=True)
    network = Column(String)
    station = Column(String)
    survey = Column(String)
    remote_path = Column(String,nullable=True,unique=True)
    remote_type = Column(Enum("s3","http"),nullable=True)
    local_path = Column(String,nullable=True,unique=True)
    type = Column(String)
    timestamp_data_start = Column(DateTime,nullable=True)
    timestamp_data_end = Column(DateTime,nullable=True)
    timestamp_created = Column(DateTime,default=datetime.datetime.now())
    parent_id = Column(String, ForeignKey("assets.uuid"),nullable=True)
