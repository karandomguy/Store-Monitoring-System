from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.config import settings
from app.database import create_tables
from app.api.routes import router

# Application lifespan events
@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"Starting {settings.app_name} v{settings.app_version}")
    
    try:
        await create_tables()
        print("Database tables ready")
    except Exception as e:
        print(f"Error creating database tables: {e}")
        raise
    
    yield    
    print("Shutting down application")


# FastAPI app
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="A high-performance API for monitoring restaurant store uptime and downtime",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routes
app.include_router(router, prefix=settings.api_prefix, tags=["Store Monitoring"])


# Root endpoint
@app.get("/", tags=["Root"])
async def root():
    return {
        "message": f"Welcome to {settings.app_name}",
        "version": settings.app_version,
        "docs": "/docs",
        "health": "/health",
        "api_health": f"{settings.api_prefix}/health",
        "api": settings.api_prefix,
        "status": "running",
        "timestamp": datetime.utcnow().isoformat()
    }


# Health endpoint
@app.get("/health", tags=["Health"])
async def root_health_check():
    return {
        "status": "healthy",
        "message": "Store Monitoring System is running",
        "version": settings.app_version,
        "timestamp": datetime.utcnow().isoformat(),
        "detailed_health": f"{settings.api_prefix}/health"
    }


# Exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc) if settings.debug else "An unexpected error occurred",
            "path": str(request.url.path),
            "timestamp": datetime.utcnow().isoformat()
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug
    )