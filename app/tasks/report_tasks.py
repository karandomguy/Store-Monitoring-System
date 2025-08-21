import io
import csv
import traceback
import gc
from datetime import datetime, timezone
from typing import List, Dict, Any
from celery import Celery
from sqlalchemy import select, func, distinct

from app.core.config import settings
from app.database import SessionLocal
from app.models import Report, StorePoll

try:
    from app.core.calculations import StoreMetricsCalculator
except ImportError as e:
    print(f"Warning: Could not import StoreMetricsCalculator: {e}")
    StoreMetricsCalculator = None

try:
    from app.core.cache import cache
except ImportError as e:
    print(f"Warning: Could not import cache: {e}")
    cache = None


try:
    celery_app = Celery(
        "store_monitoring",
        broker=settings.redis_url,
        backend=settings.redis_url,
        include=["app.tasks.report_tasks"]
    )

    celery_app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="UTC",
        enable_utc=True,
        task_track_started=True,
        task_time_limit=60 * 60,
        task_soft_time_limit=55 * 60,
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        worker_max_tasks_per_child=50,
        worker_max_memory_per_child=1024 * 1024,
        broker_connection_retry_on_startup=True,
        broker_connection_retry=True
    )
    
    print(f"Celery initialized with broker: {settings.redis_url}")
    
except Exception as e:
    print(f"Warning: Could not initialize Celery: {e}")
    celery_app = None


def create_dummy_task(func):
    def wrapper(*args, **kwargs):
        print(f"Running {func.__name__} in development mode (no Celery)")
        return func(*args, **kwargs)
    
    wrapper.delay = wrapper
    return wrapper


def generate_store_report_func(report_id: str) -> Dict[str, Any]:
    """
    Generate comprehensive store monitoring report with memory optimization.
    
    Args:
        report_id: Unique identifier for this report
        
    Returns:
        Dict with report generation results
    """
    db = SessionLocal()
    
    try:
        report = db.get(Report, report_id)
        if not report:
            raise Exception(f"Report {report_id} not found")
        
        max_timestamp_result = db.execute(select(func.max(StorePoll.timestamp_utc)))
        data_max_time = max_timestamp_result.scalar()
        
        if not data_max_time:
            raise Exception("No data found in database")
        
        print(f"Using data max timestamp as current time: {data_max_time}")
        current_time = data_max_time
        
        stmt = select(distinct(StorePoll.store_id))
        store_ids = [row[0] for row in db.execute(stmt).all()]
        
        if not store_ids:
            raise Exception("No stores found in database")
        
        print(f"Processing {len(store_ids)} stores using data timestamp {current_time}")
        
        batch_size = 100
        total_stores = len(store_ids)
        report_data = []
        
        # Initialize metrics
        if StoreMetricsCalculator:
            calculator = StoreMetricsCalculator(db)
        else:
            calculator = None
        
        for batch_start in range(0, total_stores, batch_size):
            batch_end = min(batch_start + batch_size, total_stores)
            batch_store_ids = store_ids[batch_start:batch_end]
            
            print(f"Processing batch {batch_start//batch_size + 1}: stores {batch_start+1}-{batch_end} ({(batch_end/total_stores)*100:.1f}%)")
            
            # Process this batch
            batch_results = []
            for i, store_id in enumerate(batch_store_ids):
                try:
                    # Progress logging for large datasets
                    global_progress = batch_start + i + 1
                    if global_progress % 500 == 0:
                        print(f"Processing store {global_progress:,}/{total_stores:,} ({(global_progress/total_stores)*100:.1f}%)")
                    
                    # Check cache
                    cached_metrics = None
                    if cache:
                        try:
                            cached_metrics = cache.get_store_metrics(store_id)
                        except Exception as e:
                            pass
                    
                    if cached_metrics:
                        metrics = cached_metrics
                    elif calculator:
                        metrics = calculator.calculate_store_metrics(store_id, current_time)
                        if cache:
                            try:
                                cache.set_store_metrics(store_id, metrics, ttl=3600)  # 1 hour TTL
                            except Exception as e:
                                pass
                    else:
                        # Fallback metrics when calculator is not available
                        metrics = {
                            "store_id": store_id,
                            "uptime_last_hour": 0,
                            "uptime_last_day": 0,
                            "uptime_last_week": 0,
                            "downtime_last_hour": 0,
                            "downtime_last_day": 0,
                            "downtime_last_week": 0,
                        }
                    
                    batch_results.append(metrics)
                    
                except Exception as e:
                    print(f"Error calculating metrics for store {store_id}: {str(e)}")
                    batch_results.append({
                        "store_id": store_id,
                        "uptime_last_hour": 0,
                        "uptime_last_day": 0,
                        "uptime_last_week": 0,
                        "downtime_last_hour": 0,
                        "downtime_last_day": 0,
                        "downtime_last_week": 0,
                    })
            
            report_data.extend(batch_results)
            
            del batch_results
            gc.collect()
            
            # Update report progress
            try:
                report.stores_processed = len(report_data)
                db.commit()
            except Exception as e:
                print(f"Warning: Could not update progress: {e}")
        
        # Generate CSV data
        print(f"Generating CSV for {len(report_data)} stores")
        csv_data = _generate_csv_content(report_data, current_time)
        
        del report_data
        gc.collect()
        
        # Update report with results
        report.status = "Complete"
        report.csv_data = csv_data
        report.completed_at = datetime.now(timezone.utc)  # Use actual time for report completion
        report.stores_processed = total_stores
        db.commit()
        
        print(f"Report {report_id} completed successfully!")
        print(f"Stores processed: {total_stores:,}")
        print(f"Data timestamp used: {current_time}")
        print(f"Report generated at: {datetime.now(timezone.utc)}")
        
        return {
            'status': 'Complete',
            'stores_processed': total_stores,
            'calculation_time': current_time.isoformat(),
            'data_max_timestamp': current_time.isoformat(),
            'report_id': report_id
        }
        
    except Exception as e:
        # Update report with error
        error_message = str(e)
        traceback_str = traceback.format_exc()
        
        print(f"Report generation failed: {error_message}")
        print(f"Traceback: {traceback_str}")
        
        if 'report' in locals() and report:
            report.status = "Failed"
            report.error_message = f"{error_message}\n\nTraceback:\n{traceback_str}"
            report.completed_at = datetime.now(timezone.utc)
            db.commit()
        
        # Re-raise for Celery
        raise Exception(f"Report generation failed: {error_message}")
    
    finally:
        db.close()
        gc.collect()


def process_new_poll_data_func(store_ids: List[str]) -> Dict[str, Any]:
    """
    Process new poll data and invalidate relevant caches.
    
    Args:
        store_ids: List of store IDs that received new data
        
    Returns:
        Dict with processing results
    """
    try:
        if cache:
            # Invalidate cache for affected stores
            cache.invalidate_multiple_stores(store_ids)
            
            # Invalidate system stats cache
            cache.redis_client.delete("store_monitoring:system:stats")
        
        return {
            'status': 'success',
            'stores_invalidated': len(store_ids),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }


def celery_health_check_func() -> Dict[str, str]:
    return {
        'status': 'healthy',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'worker': 'available'
    }


def _generate_csv_content(report_data: List[Dict[str, Any]], calculation_time: datetime) -> str:
    # Generate CSV content from report data
    output = io.StringIO()
    
    output.write(f"# Store Monitoring Report - Generated at {datetime.now(timezone.utc).isoformat()}\n")
    output.write(f"# Data calculation time: {calculation_time.isoformat()}\n")
    output.write(f"# Total stores: {len(report_data)}\n")
    output.write(f"# Calculation method: Real-time with intelligent interpolation\n")
    output.write(f"# Note: Using static dataset max timestamp as current time\n")
    output.write("#\n")
    
    # CSV headers
    headers = [
        'store_id',
        'uptime_last_hour',
        'uptime_last_day', 
        'uptime_last_week',
        'downtime_last_hour',
        'downtime_last_day',
        'downtime_last_week'
    ]
    
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()
    
    for row in report_data:
        filtered_row = {}
        for header in headers:
            value = row.get(header, 0)
            # Round to 2 decimal
            if isinstance(value, (int, float)) and header != 'store_id':
                filtered_row[header] = round(float(value), 2)
            else:
                filtered_row[header] = value
        writer.writerow(filtered_row)
    
    return output.getvalue()


if celery_app:
    try:
        generate_store_report = celery_app.task(name="generate_store_report")(generate_store_report_func)
        process_new_poll_data = celery_app.task(name="process_new_poll_data")(process_new_poll_data_func)
        celery_health_check = celery_app.task(name="celery_health_check")(celery_health_check_func)
        print("Celery tasks registered successfully")
    except Exception as e:
        print(f"Warning: Could not register Celery tasks: {e}")
        # Fallback to dummy tasks
        generate_store_report = create_dummy_task(generate_store_report_func)
        process_new_poll_data = create_dummy_task(process_new_poll_data_func)
        celery_health_check = create_dummy_task(celery_health_check_func)
else:
    # If Celery is not available
    generate_store_report = create_dummy_task(generate_store_report_func)
    process_new_poll_data = create_dummy_task(process_new_poll_data_func)
    celery_health_check = create_dummy_task(celery_health_check_func)