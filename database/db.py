import os
import pathlib
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from sqlalchemy import text, event
from sqlalchemy.pool import NullPool

_raw_url = os.getenv(
    "DATABASE_URL",
    "postgresql://admin:anticorrupcao123@localhost:5432/anticorrupcao"
)
ASYNC_URL = (
    _raw_url
    .replace("postgresql://", "postgresql+asyncpg://")
    .replace("postgres://", "postgresql+asyncpg://")
)

engine = create_async_engine(
    ASYNC_URL,
    echo=False,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
)

AsyncSessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db():
    """
    Executa o schema.sql na inicialização.
    Cada statement roda em sua própria transação isolada para que
    um erro (ex: objeto já existe) não aborte os demais.
    """
    schema_path = pathlib.Path(__file__).parent / "schema.sql"
    with open(schema_path, "r") as f:
        sql = f.read()

    # Divide por ; e limpa
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    # Cria engine separado com AUTOCOMMIT para DDL (necessário para CREATE EXTENSION)
    autocommit_engine = create_async_engine(
        ASYNC_URL,
        echo=False,
        poolclass=NullPool,
        execution_options={"isolation_level": "AUTOCOMMIT"},
    )

    async with autocommit_engine.connect() as conn:
        for stmt in statements:
            try:
                await conn.execute(text(stmt))
            except Exception as e:
                err = str(e).lower()
                # Ignora erros esperados de "já existe"
                if any(x in err for x in [
                    "already exists",
                    "duplicate",
                    "relation",
                    "does not exist",  # drop de objeto inexistente
                ]):
                    continue
                # Outros erros: loga mas não para a inicialização
                print(f"[init_db] Aviso ao executar statement: {e}")
                print(f"[init_db] SQL: {stmt[:100]}...")

    await autocommit_engine.dispose()
    print("[init_db] Schema inicializado com sucesso.")
