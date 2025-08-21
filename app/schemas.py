from datetime import datetime, time
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator


class StorePollBase(BaseModel):
    """Base schema for store polls"""
    store_id: str = Field(..., min_length=1, max_length=50)
    timestamp_utc: datetime
    status: str = Field(..., pattern="^(active|inactive)$")


class StorePollCreate(StorePollBase):
    """Schema for creating store polls"""
    pass


class StorePoll(StorePollBase):
    """Schema for store poll responses"""
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    created_at: datetime


class BusinessHoursBase(BaseModel):
    """Base schema for business hours"""
    store_id: str = Field(..., min_length=1, max_length=50)
    day_of_week: int = Field(..., ge=0, le=6)
    start_time_local: time
    end_time_local: time
    
    @field_validator('day_of_week')
    @classmethod
    def validate_day_of_week(cls, v):
        if not 0 <= v <= 6:
            raise ValueError('day_of_week must be between 0 (Monday) and 6 (Sunday)')
        return v


class BusinessHoursCreate(BusinessHoursBase):
    """Schema for creating business hours"""
    pass


class BusinessHours(BusinessHoursBase):
    """Schema for business hours responses"""
    model_config = ConfigDict(from_attributes=True)
    
    id: int


class StoreTimezoneBase(BaseModel):
    """Base schema for store timezones"""
    store_id: str = Field(..., min_length=1, max_length=50)
    timezone_str: str = Field(default="America/Chicago", max_length=50)


class StoreTimezoneCreate(StoreTimezoneBase):
    """Schema for creating store timezones"""
    pass


class StoreTimezone(StoreTimezoneBase):
    """Schema for store timezone responses"""
    model_config = ConfigDict(from_attributes=True)
    
    id: int


class ReportResponse(BaseModel):
    """Schema for report trigger response"""
    report_id: str


class ReportStatus(BaseModel):
    """Schema for report status response"""
    model_config = ConfigDict(from_attributes=True)
    
    report_id: str = Field(alias="id")
    status: str
    created_at: datetime
    completed_at: Optional[datetime] = None
    stores_processed: Optional[int] = None
    error_message: Optional[str] = None


class StoreMetrics(BaseModel):
    """Schema for individual store metrics"""
    store_id: str
    uptime_last_hour: float = Field(..., ge=0, description="Uptime in minutes")
    uptime_last_day: float = Field(..., ge=0, description="Uptime in hours")
    uptime_last_week: float = Field(..., ge=0, description="Uptime in hours")
    downtime_last_hour: float = Field(..., ge=0, description="Downtime in minutes")
    downtime_last_day: float = Field(..., ge=0, description="Downtime in hours")
    downtime_last_week: float = Field(..., ge=0, description="Downtime in hours")


class SystemStats(BaseModel):
    """Schema for system statistics"""
    total_stores: int
    total_observations: int
    data_range: dict[str, Optional[str]]
    current_time: datetime
    latest_data_timestamp: Optional[datetime] = None


class BulkPollCreate(BaseModel):
    """Schema for bulk poll creation"""
    polls: list[StorePollCreate] = Field(..., min_length=1, max_length=1000)


class HealthCheck(BaseModel):
    """Schema for health check response"""
    status: str = "healthy"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    version: str
    database: str = "connected"
    redis: str = "connected"