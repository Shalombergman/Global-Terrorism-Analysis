
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from src.db.models.base import Base


class WeaponType(Base):
    __tablename__ = 'weapon_types'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    
    events = relationship("TerrorEvent", back_populates="weapon_type")
