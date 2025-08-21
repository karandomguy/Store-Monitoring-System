import uuid
from datetime import datetime, timezone
from typing import List
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from fastapi.responses import Response, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, distinct
import io

from app.database import get_async_db
from app.models import Report, StorePoll, BusinessHours, StoreTimezone
from app.schemas import (
    ReportResponse, StoreMetrics, SystemStats, HealthCheck,
    BulkPollCreate, StorePollCreate, BusinessHoursCreate, StoreTimezoneCreate
)
from app.core.cache import cache
from app.core.config import settings

router = APIRouter()


# Celery tasks
try:
    from app.tasks.report_tasks import generate_store_report, process_new_poll_data
except ImportError as e:
    print(f"Warning: Could not import Celery tasks: {e}")
    # Dummy functions
    def generate_store_report(*args, **kwargs):
        print("Celery task not available - running in development mode")
        return None
    
    def process_new_poll_data(*args, **kwargs):
        print("Celery task not available - running in development mode")
        return None


# CORE REPORT ENDPOINTS
@router.post("/trigger_report", response_model=ReportResponse)
async def trigger_report(db: AsyncSession = Depends(get_async_db)):
    try:
        report_id = str(uuid.uuid4())
        
        report = Report(id=report_id, status="Running")
        db.add(report)
        await db.commit()
        
        try:
            if hasattr(generate_store_report, 'delay'):
                generate_store_report.delay(report_id)
                print(f"Celery task triggered for report {report_id}")
            else:
                print(f"No background processing for {report_id}")
        except Exception as e:
            print(f"Error triggering background task: {e}")
            report.status = "Failed"
            report.error_message = f"Failed to trigger background task: {str(e)}"
            await db.commit()
        
        return ReportResponse(report_id=report_id)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger report: {str(e)}")


@router.get("/get_report/{report_id}")
async def get_report(report_id: str, db: AsyncSession = Depends(get_async_db)):
    try:
        report = await db.get(Report, report_id)
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        if report.status == "Running":
            return {"status": "Running"}
        
        elif report.status == "Complete":
            if report.csv_data:
                # Return CSV as downloadable file
                csv_bytes = report.csv_data.encode('utf-8')
                
                return StreamingResponse(
                    io.BytesIO(csv_bytes),
                    media_type="text/csv",
                    headers={
                        "Content-Disposition": f"attachment; filename=store_report_{report_id}.csv"
                    }
                )
            else:
                return {"status": "Complete", "message": "No data available"}
        
        elif report.status == "Failed":
            return {
                "status": "Failed", 
                "error_message": report.error_message or "Unknown error occurred"
            }
        
        else:
            return {"status": report.status}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving report: {str(e)}")


# DATA INGESTION ENDPOINTS
@router.post("/ingest/polls")
async def ingest_store_polls(
    poll_data: BulkPollCreate,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_async_db)
):
    try:
        validated_polls = []
        affected_stores = set()
        
        for poll in poll_data.polls:
            if poll.timestamp_utc.tzinfo is None:
                poll.timestamp_utc = poll.timestamp_utc.replace(tzinfo=timezone.utc)
            
            validated_polls.append(StorePoll(
                store_id=poll.store_id,
                timestamp_utc=poll.timestamp_utc,
                status=poll.status.lower()
            ))
            affected_stores.add(poll.store_id)
        
        db.add_all(validated_polls)
        await db.commit()
        
        try:
            if hasattr(process_new_poll_data, 'delay'):
                process_new_poll_data.delay(list(affected_stores))
        except Exception as e:
            print(f"Error triggering background cache invalidation: {e}")
        
        return {
            "message": f"Successfully ingested {len(validated_polls)} poll records",
            "affected_stores": len(affected_stores),
            "timestamp": datetime.now(timezone.utc)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Data ingestion failed: {str(e)}")


@router.post("/ingest/business_hours")
async def ingest_business_hours(
    hours_data: List[BusinessHoursCreate],
    db: AsyncSession = Depends(get_async_db)
):
    # Bulk insert business hours data
    try:
        business_hours = [
            BusinessHours(**hours.model_dump()) for hours in hours_data
        ]
        
        db.add_all(business_hours)
        await db.commit()
        
        return {
            "message": f"Successfully ingested {len(business_hours)} business hour records",
            "timestamp": datetime.now(timezone.utc)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Business hours ingestion failed: {str(e)}")


@router.post("/ingest/timezones")
async def ingest_timezones(
    timezone_data: List[StoreTimezoneCreate],
    db: AsyncSession = Depends(get_async_db)
):
    # Bulk insert store timezone data
    try:
        timezones = [
            StoreTimezone(**tz.model_dump()) for tz in timezone_data
        ]
        
        db.add_all(timezones)
        await db.commit()
        
        return {
            "message": f"Successfully ingested {len(timezones)} timezone records",
            "timestamp": datetime.now(timezone.utc)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Timezone ingestion failed: {str(e)}")


# SYSTEM MONITORING ENDPOINTS
@router.get("/stats", response_model=SystemStats)
async def get_system_stats(db: AsyncSession = Depends(get_async_db)):
    try:
        # Check cache
        cached_stats = cache.get_system_stats()
        if cached_stats:
            return SystemStats(**cached_stats)
    except Exception as e:
        print(f"Cache error: {e}")
    
    current_time = datetime.now(timezone.utc)
    
    try:
        total_stores_result = await db.execute(
            select(func.count(distinct(StorePoll.store_id)))
        )
        total_stores = total_stores_result.scalar() or 0
        
        total_obs_result = await db.execute(select(func.count(StorePoll.id)))
        total_observations = total_obs_result.scalar() or 0
        
        min_date_result = await db.execute(select(func.min(StorePoll.timestamp_utc)))
        max_date_result = await db.execute(select(func.max(StorePoll.timestamp_utc)))
        
        min_date = min_date_result.scalar()
        max_date = max_date_result.scalar()
        
        stats = SystemStats(
            total_stores=total_stores,
            total_observations=total_observations,
            data_range={
                "min_date": min_date.isoformat() if min_date else None,
                "max_date": max_date.isoformat() if max_date else None
            },
            current_time=current_time,
            latest_data_timestamp=max_date
        )
        
        # Caching
        try:
            cache.set_system_stats(stats.model_dump())
        except Exception as e:
            print(f"Cache set error: {e}")
        
        return stats
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting system stats: {str(e)}")


@router.get("/health", response_model=HealthCheck)
async def health_check():
    # DB
    db_status = "connected"
    try:
        async with get_async_db().__anext__() as db:
            await db.execute(select(1))
    except Exception:
        db_status = "disconnected"
    
    # Redis
    try:
        redis_status = "connected" if cache.health_check() else "disconnected"
    except Exception:
        redis_status = "disconnected"
    
    overall_status = "healthy" if db_status == "connected" and redis_status == "connected" else "unhealthy"
    
    return HealthCheck(
        status=overall_status,
        version=settings.app_version,
        database=db_status,
        redis=redis_status
    )


# UTILITY ENDPOINTS
@router.get("/reports/{report_id}/status")
async def get_report_status(report_id: str, db: AsyncSession = Depends(get_async_db)):
    try:
        report = await db.get(Report, report_id)
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        return {
            "report_id": report.id,
            "status": report.status,
            "created_at": report.created_at,
            "completed_at": report.completed_at,
            "stores_processed": report.stores_processed,
            "error_message": report.error_message
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting report status: {str(e)}")


@router.get("/stores")
async def list_stores(
    limit: int = 100, 
    offset: int = 0,
    db: AsyncSession = Depends(get_async_db)
):
    try:
        stmt = select(distinct(StorePoll.store_id)).limit(limit).offset(offset)
        result = await db.execute(stmt)
        store_ids = [row[0] for row in result.all()]
        
        return {
            "stores": store_ids,
            "count": len(store_ids),
            "limit": limit,
            "offset": offset
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing stores: {str(e)}")

# Manually invalidate cache for a specific store
@router.delete("/cache/store/{store_id}")
async def invalidate_store_cache(store_id: str):
    try:
        cache.invalidate_store_cache(store_id)
        return {"message": f"Cache invalidated for store {store_id}"}
    except Exception as e:
        return {"message": f"Error invalidating cache: {str(e)}"}

# Manually invalidate all cache data
@router.delete("/cache/all")
async def invalidate_all_cache():
    try:
        cache.redis_client.flushdb()
        return {"message": "All cache data invalidated"}
    except Exception as e:
        return {"message": f"Error invalidating cache: {str(e)}"}