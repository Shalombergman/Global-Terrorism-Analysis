from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Table, MetaData
from sqlalchemy.orm import relationship
from src.db.models.base import Base


class Target(Base):
    __tablename__ = 'targets'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    type = Column(String)
    events = relationship("TerrorEvent", back_populates="target")
