from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, distinct
import pandas as pd

from app.models import StorePoll, BusinessHours, StoreTimezone, Report
from app.schemas import StorePollCreate, BusinessHoursCreate, StoreTimezoneCreate


# BULK OPERATIONS (For data loading)
def bulk_insert_store_polls(db: Session, polls_data: List[Dict[str, Any]]) -> int:
    if not polls_data:
        return 0
    
    # Convert to DataFrame for efficient processing
    df = pd.DataFrame(polls_data)
    
    # Data cleaning
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
    df['status'] = df['status'].str.strip().str.lower()
    
    # Bulk insert using pandas
    inserted_count = len(df)
    df.to_sql('store_polls', db.bind, if_exists='append', index=False, method='multi')
    
    return inserted_count


def bulk_insert_business_hours(db: Session, hours_data: List[Dict[str, Any]]) -> int:
    if not hours_data:
        return 0
    
    df = pd.DataFrame(hours_data)
    
    # Convert time strings to time objects if needed
    if 'start_time_local' in df.columns and df['start_time_local'].dtype == 'object':
        df['start_time_local'] = pd.to_datetime(df['start_time_local'], format='%H:%M:%S').dt.time
    if 'end_time_local' in df.columns and df['end_time_local'].dtype == 'object':
        df['end_time_local'] = pd.to_datetime(df['end_time_local'], format='%H:%M:%S').dt.time
    
    inserted_count = len(df)
    df.to_sql('business_hours', db.bind, if_exists='append', index=False, method='multi')
    
    return inserted_count


def bulk_insert_store_timezones(db: Session, timezone_data: List[Dict[str, Any]]) -> int:
    if not timezone_data:
        return 0
    
    df = pd.DataFrame(timezone_data)
    
    inserted_count = len(df)
    df.to_sql('store_timezones', db.bind, if_exists='append', index=False, method='multi')
    
    return inserted_count


# QUERY OPERATIONS
async def get_store_count(db: AsyncSession) -> int:
    result = await db.execute(select(func.count(distinct(StorePoll.store_id))))
    return result.scalar() or 0


async def get_observation_count(db: AsyncSession) -> int:
    result = await db.execute(select(func.count(StorePoll.id)))
    return result.scalar() or 0


async def get_data_date_range(db: AsyncSession) -> Dict[str, Optional[str]]:
    min_result = await db.execute(select(func.min(StorePoll.timestamp_utc)))
    max_result = await db.execute(select(func.max(StorePoll.timestamp_utc)))
    
    min_date = min_result.scalar()
    max_date = max_result.scalar()
    
    return {
        "min_date": min_date.isoformat() if min_date else None,
        "max_date": max_date.isoformat() if max_date else None
    }


def get_stores_with_recent_data(db: Session, hours_back: int = 2) -> List[str]:
    from datetime import datetime, timedelta
    
    cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
    
    stmt = select(distinct(StorePoll.store_id)).where(
        StorePoll.timestamp_utc >= cutoff_time
    )
    
    result = db.execute(stmt)
    return [row[0] for row in result.all()]


# REPORT OPERATIONS
def create_report(db: Session, report_id: str) -> Report:
    report = Report(id=report_id, status="Running")
    db.add(report)
    db.commit()
    db.refresh(report)
    return report


async def get_report(db: AsyncSession, report_id: str) -> Optional[Report]:
    return await db.get(Report, report_id)


def update_report_status(
    db: Session, 
    report_id: str, 
    status: str, 
    csv_data: Optional[str] = None,
    error_message: Optional[str] = None,
    stores_processed: Optional[int] = None
) -> Optional[Report]:

    from datetime import datetime
    
    report = db.get(Report, report_id)
    if not report:
        return None
    
    report.status = status
    if csv_data:
        report.csv_data = csv_data
    if error_message:
        report.error_message = error_message
    if stores_processed is not None:
        report.stores_processed = stores_processed
    
    if status in ["Complete", "Failed"]:
        report.completed_at = datetime.utcnow()
    
    db.commit()
    return report


# DATA VALIDATION
def validate_store_polls_data(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not data:
        return {"valid": False, "errors": ["No data provided"]}
    
    errors = []
    valid_count = 0
    
    required_fields = ['store_id', 'timestamp_utc', 'status']
    valid_statuses = {'active', 'inactive'}
    
    for i, record in enumerate(data):
        # Check required fields
        missing_fields = [field for field in required_fields if field not in record]
        if missing_fields:
            errors.append(f"Record {i}: Missing fields: {missing_fields}")
            continue
        
        # Validate status
        if record['status'].lower() not in valid_statuses:
            errors.append(f"Record {i}: Invalid status '{record['status']}'. Must be 'active' or 'inactive'")
            continue
        
        # Validate store_id
        if not record['store_id'] or not isinstance(record['store_id'], str):
            errors.append(f"Record {i}: Invalid store_id")
            continue
        
        valid_count += 1
    
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "total_records": len(data),
        "valid_records": valid_count
    }