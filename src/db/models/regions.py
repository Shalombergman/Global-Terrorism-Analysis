from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Table, MetaData
from sqlalchemy.orm import relationship
from src.db.models.base import Base


class Region(Base):
    __tablename__ = 'regions'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    country = Column(String, nullable=False)
    state = Column(String)
    city = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    events = relationship("TerrorEvent", back_populates="region")
