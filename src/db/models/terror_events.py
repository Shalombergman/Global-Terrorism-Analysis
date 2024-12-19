from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Table, MetaData
from sqlalchemy.orm import relationship
from src.db.models.base import Base


class TerrorEvent(Base):
    __tablename__ = 'terror_events'
    
    event_id = Column(String, primary_key=True)
    date = Column(DateTime, nullable=False)
    
    region_id = Column(Integer, ForeignKey('regions.id'), nullable=False)
    group_id = Column(Integer, ForeignKey('groups.id'))
    attack_type_id = Column(Integer, ForeignKey('attack_types.id'), nullable=False)
    target_id = Column(Integer, ForeignKey('targets.id'))
    weapon_type_id = Column(Integer, ForeignKey('weapon_types.id'))
    
    killed = Column(Integer, default=0)
    wounded = Column(Integer, default=0)
    casualties = Column(Integer)
    severity_score = Column(Float)
    
    summary = Column(String)
    motive = Column(String)
    success = Column(Boolean, default=False)
    num_perpetrators = Column(Integer)
    
    # Relationships
    region = relationship("Region", back_populates="events")
    group = relationship("Group", back_populates="events")
    attack_type = relationship("AttackType", back_populates="events")
    target = relationship("Target", back_populates="events")
    weapon_type = relationship("WeaponType", back_populates="events")