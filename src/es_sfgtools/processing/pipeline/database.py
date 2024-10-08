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
    parent_id = Column(Integer, ForeignKey("assets.id"),nullable=True)


class MultiAssets(Base):
    __tablename__ = "multiassets"
    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String)
    timestamp_data_start = Column(DateTime)
    timestamp_data_end = Column(DateTime)
    network = Column(String)
    station = Column(String)
    survey = Column(String)
    parent_type = Column(String) # SV3, SV3,SV3_QC
    local_path = Column(String, nullable=True, unique=True)
    is_updated = Column(Boolean, default=False)
    parent_id = Column(String)

class ModelResults(Base):
    __tablename__ = "modelresults"
    id = Column(Integer, primary_key=True, autoincrement=True)
    asset_id = Column(Integer, ForeignKey("multiassets.id"))
    asset_parent_type = Column(String, ForeignKey("multiassets.parent_type"))
    asset_local_path = Column(String,ForeignKey("multiassets.local_path"))
    sound_velocity_path = Column(String, ForeignKey("multiassets.local_path"))
    hyper_params = Column(JSON)
    rms_tt = Column(Float)
    abic = Column(Float)
    delta_center_position = Column(JSON)
    
    #delta_center_position = Column(JSON)
    #rms = Column(Float)
    #abic = Column(Float)
    #data_start_time
    #data_end_time
    #created_time