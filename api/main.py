import sys
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from core.cache.redis_client import RedisClient
from core.db.postgres import PostgresClient
from core.utils.logger import get_logger
from api.routes.indicators import router as indicators_router

logger = get_logger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize clients and store in app.state
    app.state.redis = RedisClient()
    app.state.postgres = PostgresClient()
    
    await app.state.redis.connect()
    await app.state.postgres.create_pool()
    logger.info("API dependencies initialized")
    yield
    await app.state.redis.close()
    await app.state.postgres.close()
    logger.info("API dependencies cleaned up")


app = FastAPI(
    title="Crypto Pipeline API",
    description="Real-time crypto market data indicators",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


app.include_router(indicators_router, prefix="/indicators", tags=["indicators"])
