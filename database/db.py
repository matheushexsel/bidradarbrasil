import os
import pathlib
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from sqlalchemy import text

# Railway injeta DATABASE_URL como postgresql://...
# SQLAlchemy async precisa de postgresql+asyncpg://
_raw_url = os.getenv(
    "DATABASE_URL",
    "postgresql://admin:anticorrupcao123@localhost:5432/anticorrupcao"
)
ASYNC_URL = (
    _raw_url
    .replace("postgresql://", "postgresql+asyncpg://")
    .replace("postgres://", "postgresql+asyncpg://")
)

engine = create_async_engine(ASYNC_URL, echo=False, pool_size=10, max_overflow=20, pool_pre_ping=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db():
    """Executa o schema.sql na inicialização."""
    schema_path = pathlib.Path(__file__).parent / "schema.sql"
    with open(schema_path, "r") as f:
        sql = f.read()

    statements = [s.strip() for s in sql.split(";") if s.strip()]
    async with engine.begin() as conn:
        for stmt in statements:
            try:
                await conn.execute(text(stmt))
            except Exception as e:
                if "already exists" not in str(e).lower():
                    raise
