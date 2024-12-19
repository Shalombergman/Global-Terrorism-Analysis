from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Table, MetaData
from sqlalchemy.orm import relationship
from src.db.models.base import Base


class Group(Base):
    __tablename__ = 'groups'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False) 
    events = relationship("TerrorEvent", back_populates="group")
