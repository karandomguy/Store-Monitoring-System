# Store Monitoring System

A FastAPI application that generates uptime/downtime reports for restaurant stores based on polling data.

## Requirements

- Docker and Docker Compose
- 8GB+ RAM (for processing large datasets)

## Quick Start

1. **Start the services:**
```bash
docker-compose up -d
```

2. **Load the data:**
```bash
docker-compose exec app python scripts/load_data.py
```

3. **Generate a report:**
```bash
curl -X POST http://localhost:8000/api/v1/trigger_report
```

4. **Check report status (use the report_id from step 3):**
```bash
curl http://localhost:8000/api/v1/get_report/{report_id}
```

## API Endpoints

- `POST /api/v1/trigger_report` - Start report generation
- `GET /api/v1/get_report/{report_id}` - Get report status or download CSV
- `GET /api/v1/stats` - System statistics
- `GET /api/v1/health` - Health check
- `GET /docs` - API documentation

## Output Format

The system generates CSV reports with the following columns:
- `store_id`
- `uptime_last_hour` (minutes)
- `uptime_last_day` (hours)
- `uptime_last_week` (hours)
- `downtime_last_hour` (minutes)
- `downtime_last_day` (hours)
- `downtime_last_week` (hours)

## Features

- **Business Hours Filtering**: Only counts uptime/downtime during store operating hours
- **Timezone Support**: Handles stores across different timezones with DST awareness  
- **Smart Interpolation**: Extrapolates uptime/downtime from sparse polling data
- **Background Processing**: Report generation runs asynchronously via Celery
- **Caching**: Redis caching for improved performance on large datasets
- **Batch Processing**: Efficiently handles thousands of stores without memory issues

## How It Works

<img width="960" height="889" alt="Flowchart" src="https://github.com/user-attachments/assets/994d84f2-d095-44e6-9f23-3adb4758837f" />

The system:
1. Downloads store polling data, business hours, and timezone information
2. Calculates uptime/downtime using business hours filtering
3. Interpolates status between sparse polling observations
4. Generates reports with metrics for last hour, day, and week

## Data Sources

The application automatically downloads the official dataset from:
https://storage.googleapis.com/hiring-problem-statements/store-monitoring-data.zip

## Testing

Check if everything is working:
```bash
# Health check
curl http://localhost:8000/health

# System stats
curl http://localhost:8000/api/v1/stats

# Generate and download a report
REPORT_ID=$(curl -s -X POST http://localhost:8000/api/v1/trigger_report | jq -r .report_id)
curl http://localhost:8000/api/v1/get_report/$REPORT_ID
```

## Services

- **API**: FastAPI server on port 8000
- **Database**: PostgreSQL for data storage
- **Cache**: Redis for performance
- **Workers**: Celery for background report generation

## Stopping

```bash
docker-compose down
```
