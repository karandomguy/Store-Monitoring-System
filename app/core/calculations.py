from datetime import datetime, timedelta, time, timezone
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
import pytz
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from app.models import StorePoll, BusinessHours, StoreTimezone


@dataclass
class TimeInterval:
    """Represents a time interval with a status."""
    start: datetime
    end: datetime
    status: str


@dataclass
class BusinessPeriod:
    """Represents a business operating period."""
    start: datetime
    end: datetime
    day_of_week: int


class StoreMetricsCalculator:
    """Calculates store uptime/downtime metrics with intelligent interpolation."""
    
    def __init__(self, db: Session):
        self.db = db
        self._timezone_cache = {}
        self._business_hours_cache = {}
        self._max_timestamp = None
    
    def ensure_timezone_aware(self, dt: datetime) -> datetime:
        """Ensure datetime is timezone-aware (UTC if no timezone)."""
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    
    def ensure_timezone_naive(self, dt: datetime) -> datetime:
        """Ensure datetime is timezone-naive."""
        if dt.tzinfo is not None:
            return dt.replace(tzinfo=None)
        return dt
    
    def get_max_timestamp(self) -> datetime:
        """Get the maximum timestamp from the data to use as 'current time' for static datasets."""
        if self._max_timestamp is None:
            result = self.db.execute(select(func.max(StorePoll.timestamp_utc)))
            max_ts = result.scalar()
            if max_ts:
                self._max_timestamp = self.ensure_timezone_aware(max_ts)
                print(f"Using static dataset max timestamp: {self._max_timestamp}")
            else:
                # If no data
                self._max_timestamp = datetime.now(timezone.utc)
                print("Using actual current time")
        
        return self._max_timestamp
    
    def get_store_timezone(self, store_id: str) -> pytz.timezone:
        """Get store timezone with caching."""
        if store_id not in self._timezone_cache:
            stmt = select(StoreTimezone).where(StoreTimezone.store_id == store_id)
            tz_record = self.db.execute(stmt).scalar_one_or_none()
            
            tz_str = tz_record.timezone_str if tz_record else "America/Chicago"
            self._timezone_cache[store_id] = pytz.timezone(tz_str)
        
        return self._timezone_cache[store_id]
    
    def get_business_hours(self, store_id: str) -> Dict[int, Tuple[time, time]]:
        """Get business hours for a store with caching."""
        if store_id not in self._business_hours_cache:
            stmt = select(BusinessHours).where(BusinessHours.store_id == store_id)
            hours = self.db.execute(stmt).scalars().all()
            
            if not hours:
                default_hours = {i: (time(0, 0), time(23, 59, 59)) for i in range(7)}
                self._business_hours_cache[store_id] = default_hours
            else:
                self._business_hours_cache[store_id] = {
                    h.day_of_week: (h.start_time_local, h.end_time_local) 
                    for h in hours
                }
        
        return self._business_hours_cache[store_id]
    
    def get_business_periods(
        self, store_id: str, start_time: datetime, end_time: datetime
    ) -> List[BusinessPeriod]:
        """Generate all business periods within the given time range."""
        start_time = self.ensure_timezone_aware(start_time)
        end_time = self.ensure_timezone_aware(end_time)
        
        store_tz = self.get_store_timezone(store_id)
        business_hours = self.get_business_hours(store_id)
        
        # Convert to local timezone
        local_start = start_time.astimezone(store_tz)
        local_end = end_time.astimezone(store_tz)
        
        periods = []
        current_date = local_start.date()
        
        while current_date <= local_end.date():
            day_of_week = current_date.weekday()
            
            if day_of_week in business_hours:
                start_time_local, end_time_local = business_hours[day_of_week]
                
                try:
                    business_start = store_tz.localize(
                        datetime.combine(current_date, start_time_local)
                    )
                    business_end = store_tz.localize(
                        datetime.combine(current_date, end_time_local)
                    )
                except Exception:
                    # Handle ambiguous times (DST transitions)
                    business_start = store_tz.localize(
                        datetime.combine(current_date, start_time_local),
                        is_dst=None
                    )
                    business_end = store_tz.localize(
                        datetime.combine(current_date, end_time_local),
                        is_dst=None
                    )
                
                # end_time < start_time (overnight businesses)
                if end_time_local < start_time_local:
                    business_end += timedelta(days=1)
                
                business_start_utc = business_start.astimezone(timezone.utc)
                business_end_utc = business_end.astimezone(timezone.utc)
                local_start_utc = local_start.astimezone(timezone.utc)
                local_end_utc = local_end.astimezone(timezone.utc)
                
                period_start = max(business_start_utc, local_start_utc)
                period_end = min(business_end_utc, local_end_utc)
                
                if period_start < period_end:
                    periods.append(BusinessPeriod(
                        start=period_start,
                        end=period_end,
                        day_of_week=day_of_week
                    ))
            
            current_date += timedelta(days=1)
        
        return periods
    
    def get_store_obs(
        self, store_id: str, start_time: datetime, end_time: datetime
    ) -> List[StorePoll]:
        """Get all obs for a store within time range."""
        start_time = self.ensure_timezone_aware(start_time)
        end_time = self.ensure_timezone_aware(end_time)
        
        stmt = select(StorePoll).where(
            StorePoll.store_id == store_id,
            StorePoll.timestamp_utc >= start_time,
            StorePoll.timestamp_utc <= end_time
        ).order_by(StorePoll.timestamp_utc)
        
        obs = list(self.db.execute(stmt).scalars().all())
        
        for obs in obs:
            obs.timestamp_utc = self.ensure_timezone_aware(obs.timestamp_utc)
        
        return obs
    
    def interpolate_status_for_period(
        self, period: BusinessPeriod, obs: List[StorePoll]
    ) -> List[TimeInterval]:
        """
        Core interpolation logic: Convert sparse obs into continuous time intervals.
        """
        period_start = self.ensure_timezone_aware(period.start)
        period_end = self.ensure_timezone_aware(period.end)
        
        # Filter obs
        period_obs = []
        for obs in obs:
            obs_time = self.ensure_timezone_aware(obs.timestamp_utc)
            if period_start <= obs_time <= period_end:
                period_obs.append(obs)
        
        if not period_obs:
            return self._handle_no_obs(period, obs)
        
        if len(period_obs) == 1:
            return self._handle_single_obs(period, period_obs[0])
        
        return self._handle_multiple_obs(period, period_obs)
    
    def _handle_no_obs(
        self, period: BusinessPeriod, all_obs: List[StorePoll]
    ) -> List[TimeInterval]:
        """Handle business period with no obs."""
        period_start = self.ensure_timezone_aware(period.start)
        period_end = self.ensure_timezone_aware(period.end)
        
        before_obs = []
        after_obs = []
        
        for obs in all_obs:
            obs_time = self.ensure_timezone_aware(obs.timestamp_utc)
            if obs_time < period_start:
                before_obs.append(obs)
            elif obs_time > period_end:
                after_obs.append(obs)
        
        if before_obs:
            # Status from the last obs
            status = before_obs[-1].status
        elif after_obs:
            # Status from the first obs
            status = after_obs[0].status
        else:
            # No data
            status = "inactive"
        
        return [TimeInterval(start=period_start, end=period_end, status=status)]
    
    def _handle_single_obs(
        self, period: BusinessPeriod, obs: StorePoll
    ) -> List[TimeInterval]:
        """Handle business period with single obs."""
        period_start = self.ensure_timezone_aware(period.start)
        period_end = self.ensure_timezone_aware(period.end)
        obs_time = self.ensure_timezone_aware(obs.timestamp_utc)
        
        intervals = []
        
        # Time before the obs
        if obs_time > period_start:
            opposite_status = "inactive" if obs.status == "active" else "active"
            intervals.append(TimeInterval(
                start=period_start,
                end=obs_time,
                status=opposite_status
            ))
        
        # Time from obs to end
        if obs_time < period_end:
            intervals.append(TimeInterval(
                start=obs_time,
                end=period_end,
                status=obs.status
            ))
        
        # If obs is exactly at period boundaries
        if not intervals:
            intervals.append(TimeInterval(
                start=period_start,
                end=period_end,
                status=obs.status
            ))
        
        return intervals
    
    def _handle_multiple_obs(
        self, period: BusinessPeriod, obs: List[StorePoll]
    ) -> List[TimeInterval]:
        """Handle business period with multiple obs."""
        period_start = self.ensure_timezone_aware(period.start)
        period_end = self.ensure_timezone_aware(period.end)
        
        intervals = []
        
        obs = sorted(obs, key=lambda x: self.ensure_timezone_aware(x.timestamp_utc))
        
        first_obs = obs[0]
        first_obs_time = self.ensure_timezone_aware(first_obs.timestamp_utc)
        
        if first_obs_time > period_start:
            intervals.append(TimeInterval(
                start=period_start,
                end=first_obs_time,
                status=first_obs.status
            ))
        
        # Intervals between consecutive obs
        for i in range(len(obs) - 1):
            current_obs = obs[i]
            next_obs = obs[i + 1]
            
            current_time = self.ensure_timezone_aware(current_obs.timestamp_utc)
            next_time = self.ensure_timezone_aware(next_obs.timestamp_utc)
            
            # Status change happens at midpoint between obs if they differ
            if current_obs.status != next_obs.status:
                midpoint = current_time + (next_time - current_time) / 2
                
                intervals.append(TimeInterval(
                    start=current_time,
                    end=midpoint,
                    status=current_obs.status
                ))
                intervals.append(TimeInterval(
                    start=midpoint,
                    end=next_time,
                    status=next_obs.status
                ))
            else:
                # Same status, single interval
                intervals.append(TimeInterval(
                    start=current_time,
                    end=next_time,
                    status=current_obs.status
                ))
        
        # Add interval from last obs to period end
        last_obs = obs[-1]
        last_obs_time = self.ensure_timezone_aware(last_obs.timestamp_utc)
        
        if last_obs_time < period_end:
            intervals.append(TimeInterval(
                start=last_obs_time,
                end=period_end,
                status=last_obs.status
            ))
        
        return self._merge_adjacent_intervals(intervals)
    
    def _merge_adjacent_intervals(self, intervals: List[TimeInterval]) -> List[TimeInterval]:
        """Merge adjacent intervals with the same status."""
        if not intervals:
            return []
        
        merged = [intervals[0]]
        
        for interval in intervals[1:]:
            last_merged = merged[-1]
            
            last_end = self.ensure_timezone_aware(last_merged.end)
            interval_start = self.ensure_timezone_aware(interval.start)
            
            if (last_merged.status == interval.status and 
                abs((last_end - interval_start).total_seconds()) < 1):  # Allow small gaps
                # Merge intervals
                merged[-1] = TimeInterval(
                    start=last_merged.start,
                    end=interval.end,
                    status=last_merged.status
                )
            else:
                merged.append(interval)
        
        return merged
    
    def calculate_uptime_downtime(self, intervals: List[TimeInterval]) -> Tuple[float, float]:
        """Calculate total uptime and downtime in minutes."""
        uptime_minutes = 0.0
        downtime_minutes = 0.0
        
        for interval in intervals:
            start_time = self.ensure_timezone_aware(interval.start)
            end_time = self.ensure_timezone_aware(interval.end)
            
            duration_minutes = (end_time - start_time).total_seconds() / 60
            
            if interval.status == "active":
                uptime_minutes += duration_minutes
            else:
                downtime_minutes += duration_minutes
        
        return uptime_minutes, downtime_minutes
    
    def calculate_store_metrics(
        self, store_id: str, current_time: Optional[datetime] = None
    ) -> Dict[str, float]:
        """Calculate all metrics for a single store."""
        try:
            if current_time is None:
                current_time = self.get_max_timestamp()
            else:
                current_time = self.ensure_timezone_aware(current_time)
            
            # Time ranges from current time
            last_hour_start = current_time - timedelta(hours=1)
            last_day_start = current_time - timedelta(days=1)
            last_week_start = current_time - timedelta(weeks=1)
            
            all_obs = self.get_store_obs(store_id, last_week_start, current_time)
            
            # Return zero
            if not all_obs:
                return {
                    "store_id": store_id,
                    "uptime_last_hour": 0.0,
                    "uptime_last_day": 0.0,
                    "uptime_last_week": 0.0,
                    "downtime_last_hour": 0.0,
                    "downtime_last_day": 0.0,
                    "downtime_last_week": 0.0
                }
            
            metrics = {}
            
            # For each time period
            time_periods = [
                ("last_hour", last_hour_start),
                ("last_day", last_day_start), 
                ("last_week", last_week_start)
            ]
            
            for period_name, start_time in time_periods:
                business_periods = self.get_business_periods(store_id, start_time, current_time)
                
                total_uptime = 0.0
                total_downtime = 0.0
                
                for period in business_periods:
                    intervals = self.interpolate_status_for_period(period, all_obs)
                    
                    # Uptime/downtime
                    uptime, downtime = self.calculate_uptime_downtime(intervals)
                    total_uptime += uptime
                    total_downtime += downtime
                
                # Required units
                if period_name == "last_hour":
                    metrics[f"uptime_{period_name}"] = total_uptime  # minutes
                    metrics[f"downtime_{period_name}"] = total_downtime  # minutes
                else:
                    metrics[f"uptime_{period_name}"] = total_uptime / 60  # hours
                    metrics[f"downtime_{period_name}"] = total_downtime / 60  # hours
            
            return {
                "store_id": store_id,
                "uptime_last_hour": metrics["uptime_last_hour"],
                "uptime_last_day": metrics["uptime_last_day"],
                "uptime_last_week": metrics["uptime_last_week"],
                "downtime_last_hour": metrics["downtime_last_hour"],
                "downtime_last_day": metrics["downtime_last_day"],
                "downtime_last_week": metrics["downtime_last_week"]
            }
            
        except Exception as e:
            # Log the error but don't crash
            print(f"Error calculating metrics for store {store_id}: {str(e)}")
            return {
                "store_id": store_id,
                "uptime_last_hour": 0.0,
                "uptime_last_day": 0.0,
                "uptime_last_week": 0.0,
                "downtime_last_hour": 0.0,
                "downtime_last_day": 0.0,
                "downtime_last_week": 0.0
            }