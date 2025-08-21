from datetime import datetime, time
from typing import Optional
from sqlalchemy import String, DateTime, Time, Integer, Text, Index, func
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base
import uuid


class StorePoll(Base):
    """Store polling data"""
    
    __tablename__ = "store_polls"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    store_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    timestamp_utc: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    status: Mapped[str] = mapped_column(String(10), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    
    __table_args__ = (
        Index('idx_store_polls_store_time', 'store_id', 'timestamp_utc'),
        Index('idx_store_polls_time', 'timestamp_utc'),
        Index('idx_store_polls_status', 'status'),
    )


class BusinessHours(Base):
    """Store business hours by day of week"""
    
    __tablename__ = "business_hours"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    store_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    day_of_week: Mapped[int] = mapped_column(Integer, nullable=False)
    start_time_local: Mapped[time] = mapped_column(Time, nullable=False)
    end_time_local: Mapped[time] = mapped_column(Time, nullable=False)
    
    __table_args__ = (
        Index('idx_business_hours_store_day', 'store_id', 'day_of_week'),
    )


class StoreTimezone(Base):
    """Store timezone information"""
    
    __tablename__ = "store_timezones"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    store_id: Mapped[str] = mapped_column(String(50), nullable=False, unique=True, index=True)
    timezone_str: Mapped[str] = mapped_column(String(50), nullable=False, default="America/Chicago")


class Report(Base):
    """Report generation tracking and storage"""
    
    __tablename__ = "reports"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="Running")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    csv_data: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stores_processed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    __table_args__ = (
        Index('idx_reports_status', 'status'),
        Index('idx_reports_created', 'created_at'),
    )