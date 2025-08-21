"""
Downloads official dataset and loads into database
"""

import os
import sys
import pandas as pd
import time
import requests
import zipfile
from pathlib import Path
from datetime import datetime

# Parent directory
sys.path.append(str(Path(__file__).parent.parent))

from app.database import SessionLocal, sync_engine, Base
from app.models import StorePoll, BusinessHours, StoreTimezone


def download_and_extract_data():
    print("Downloading dataset")
    
    data_dir = Path("/app/data")
    data_dir.mkdir(exist_ok=True)
    
    zip_path = data_dir / "store-monitoring-data.zip"
    
    # Data already exists
    if (data_dir / "store_status.csv").exists():
        return str(data_dir)
    
    # Zip file
    url = "https://storage.googleapis.com/hiring-problem-statements/store-monitoring-data.zip"
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        # Extract
        print("Extracting files")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(data_dir)
        
        # Remove zip file
        zip_path.unlink()
        
        # List extracted files
        files = list(data_dir.glob("*.csv"))
        print(f"Extracted files: {[f.name for f in files]}")
        
        return str(data_dir)
        
    except Exception as e:
        print(f"Error downloading data: {e}")
        raise


def create_tables():
    print("Creating database tables...")
    try:
        Base.metadata.create_all(bind=sync_engine)
    except Exception as e:
        print(f"Error creating tables: {e}")
        raise


def load_store_polls(file_path: str) -> int:
    total_inserted = 0
    chunk_size = 10000
    
    try:
        with open(file_path, 'r') as f:
            total_lines = sum(1 for _ in f) - 1
        
        print(f"Total records: {total_lines:,}")
        start_time = time.time()
        
        # Process in chunks
        for i, chunk_df in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
            # Clean data
            chunk_df.columns = chunk_df.columns.str.strip()
            chunk_df['timestamp_utc'] = pd.to_datetime(chunk_df['timestamp_utc'])
            chunk_df['status'] = chunk_df['status'].str.strip().str.lower()
            
            # Insert to database
            db = SessionLocal()
            try:
                records = []
                for _, row in chunk_df.iterrows():
                    records.append(StorePoll(
                        store_id=str(row['store_id']),
                        timestamp_utc=row['timestamp_utc'],
                        status=str(row['status'])
                    ))
                
                db.bulk_save_objects(records)
                db.commit()
                
                total_inserted += len(records)
                progress = (total_inserted / total_lines) * 100
                print(f"Progress: {total_inserted:,}/{total_lines:,} ({progress:.1f}%)")
                
            except Exception as e:
                print(f"Error in chunk {i}: {e}")
                db.rollback()
            finally:
                db.close()
        
        elapsed = time.time() - start_time
        print(f"Completed: {total_inserted:,} records in {elapsed:.1f}s")
        return total_inserted
        
    except Exception as e:
        print(f"Error loading store polls: {e}")
        return 0


def load_business_hours(file_path: str) -> int:
    print(f"Loading business hours from {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip()
        
        if 'dayOfWeek' in df.columns:
            df = df.rename(columns={'dayOfWeek': 'day_of_week'})
        
        # Convert times
        df['start_time_local'] = pd.to_datetime(df['start_time_local'], format='%H:%M:%S').dt.time
        df['end_time_local'] = pd.to_datetime(df['end_time_local'], format='%H:%M:%S').dt.time
        
        # Insert to database
        db = SessionLocal()
        try:
            records = []
            for _, row in df.iterrows():
                records.append(BusinessHours(
                    store_id=str(row['store_id']),
                    day_of_week=int(row['day_of_week']),
                    start_time_local=row['start_time_local'],
                    end_time_local=row['end_time_local']
                ))
            
            db.bulk_save_objects(records)
            db.commit()
            
            print(f"Loaded: {len(records):,} records")
            return len(records)
            
        except Exception as e:
            print(f"Error: {e}")
            db.rollback()
            return 0
        finally:
            db.close()
            
    except Exception as e:
        print(f"Error loading business hours: {e}")
        return 0


def load_timezones(file_path: str) -> int:
    print(f"Loading timezones from {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip()
        
        # Insert to database
        db = SessionLocal()
        try:
            records = []
            for _, row in df.iterrows():
                records.append(StoreTimezone(
                    store_id=str(row['store_id']),
                    timezone_str=str(row['timezone_str'])
                ))
            
            db.bulk_save_objects(records)
            db.commit()
            
            print(f"Loaded: {len(records):,} records")
            return len(records)
            
        except Exception as e:
            print(f"Error: {e}")
            db.rollback()
            return 0
        finally:
            db.close()
            
    except Exception as e:
        print(f"Error loading timezones: {e}")
        return 0


def print_summary():
    try:
        db = SessionLocal()
        from sqlalchemy import func, distinct
        
        store_count = db.query(func.count(distinct(StorePoll.store_id))).scalar() or 0
        poll_count = db.query(func.count(StorePoll.id)).scalar() or 0
        hours_count = db.query(func.count(BusinessHours.id)).scalar() or 0
        tz_count = db.query(func.count(StoreTimezone.id)).scalar() or 0
        
        print("Summary:")
        print(f"Stores: {store_count:,}")
        print(f"Observations: {poll_count:,}")
        print(f"Business hours: {hours_count:,}")
        print(f"Timezones: {tz_count:,}")
        
        db.close()
        
    except Exception as e:
        print(f"Error getting summary: {e}")


def main():
    print("=" * 50)
    print("Store Monitoring System - Data Loading")
    print("=" * 50)
    
    start_time = time.time()
    
    try:
        # Download and extract data
        data_dir = download_and_extract_data()
        create_tables()
        total_loaded = 0
        
        files_to_load = [
            ("store_status.csv", "Store Polls", load_store_polls),
            ("menu_hours.csv", "Business Hours", load_business_hours),
            ("timezones.csv", "Timezones", load_timezones)
        ]
        
        for filename, description, load_func in files_to_load:
            file_path = os.path.join(data_dir, filename)
            if os.path.exists(file_path):
                loaded = load_func(file_path)
                total_loaded += loaded
            else:
                print(f"{filename} not found")
        
        elapsed = time.time() - start_time
        
        if total_loaded > 0:
            print(f"Loaded {total_loaded:,} records in {elapsed:.1f}s")
            print_summary()
        else:
            print("No data loaded")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()