"""
This module contains the database models for the data management module.
"""
import datetime
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    ForeignKey,
    Boolean,
    Text,
    JSON,
    Enum

)
from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import Mapped, declarative_base, relationship,mapped_column
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()

class Assets(Base):
    """
    A class to represent the assets table.
    """
    __tablename__ = "assets"
    id = Column(Integer,primary_key=True,autoincrement=True)
    network = Column(String)
    station = Column(String)
    campaign = Column(String)
    remote_path = Column(String,nullable=True,unique=True)
    remote_type = Column(String,nullable=True)#Column(Enum("s3","http"),nullable=True)
    local_path = Column(String,nullable=True,unique=True)
    type = Column(String)
    timestamp_data_start = Column(DateTime,nullable=True)
    timestamp_data_end = Column(DateTime,nullable=True)
    timestamp_created = Column(DateTime,default=datetime.datetime.now())
    parent_id = Column(Integer, ForeignKey("assets.id"),nullable=True)
    is_processed = Column(Boolean,default=False)


class ModelResults(Base):
    """
    A class to represent the modelresults table.
    """
    __tablename__ = "modelresults"
    id = Column(Integer, primary_key=True, autoincrement=True)
    asset_id = Column(Integer, ForeignKey("assets.id"))
    asset_local_path = Column(String,ForeignKey("assets.local_path"))
    hyper_params = Column(JSON)
    rms_tt = Column(Float)
    abic = Column(Float)
    delta_center_position = Column(JSON)
    
class MergeJobs(Base):
    """
    A class to represent the mergejobs table.
    """
    __tablename__ = "mergejobs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    child_type = Column(String)
    parent_ids = Column(String)
    parent_type = Column(String)
