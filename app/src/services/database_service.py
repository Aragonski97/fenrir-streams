import asyncio

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
    AsyncConnection,
    AsyncEngine
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import URL
from typing import AsyncGenerator
from contextlib import asynccontextmanager
from structlog import get_logger

from fenrir_streams.schemas.app_config import app_config

from traceback import format_exc



class SQLDatabase:

    def __init__(
            self,
            driver: str,
            username: str,
            password: str,
            hostname: str,
            port: int,
            database: str,
            extra_logging: dict = None,

    ) -> None:
        self.driver = driver
        self.username = username
        self.password = password
        self.hostname = hostname
        self.port = port
        self.database = database

        self.connection_url = URL.create(
            drivername=driver,
            username=username,
            password=password,
            host=hostname,
            port=port,
            database=database,
        )

        self.engine: AsyncEngine | None= None
        self.sessionmaker: async_sessionmaker | None = None
        self.extra_logging = extra_logging if extra_logging is not None else {}
        self.logger = get_logger()

    async def connect(self):
        assert self.connection_url is not None
        # disconnect engine after an hour, disconnect session/connection objects after 20 minutes
        try:
            await self.engine.dispose(close=True)
        except Exception as e:
            _ = e
            pass
        self.engine = create_async_engine(
            url=self.connection_url,
            pool_size=12*app_config.number_of_coroutines,
            pool_pre_ping=True,
            pool_recycle=3600,
            max_overflow=10,
            #isolation_level="READ UNCOMMITTED"
        )
        assert await self.is_connected()
        self.sessionmaker = async_sessionmaker(bind=self.engine, expire_on_commit=False, autoflush=True)
        self.logger.info("connect to database", desc=f"connection to {self.database} has been established")

    async def is_connected(self) -> bool:
        try:
            async with self.get_connection() as check:
                await asyncio.sleep(0)
                self.logger.info(f"{self.database} is connected")
                _ = check
            return True
        except SQLAlchemyError as err:
            self.logger.warning("checking connection failed", err=err)
            return False
        except Exception as err:
            self.logger.warning(
                "checking connection",
                action="connection returned unusual error",
                err=err,
                tb=format_exc()
            )
            raise

    async def reconnect(self, max_retries: int = 3, backoff: int = None):
        self.logger.info("reconnect to database", action="starting")
        retry = 0
        sleep_time = 5
        while retry < max_retries:
            try:
                assert self.connection_url is not None
                if self.is_connected():
                    return
                await self.connect()
                self.logger.warning("reconnect to database", action=f"connection to {self.database} has been established")
            except (SQLAlchemyError, AssertionError) as err:
                self.logger.warning(
                    f"couldn't reconnect to database: {self.database}",
                    action=f"sleeping for {sleep_time} seconds",
                    err=str(err)
                )
                await asyncio.sleep(sleep_time)
                sleep_time *= 1 if backoff is None else backoff
                continue
            else:
                self.logger.info("reconnect to database", action="finished")
                return
        raise ConnectionError(f"couldn't connect to database after {max_retries} retries")

    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        session = self.sessionmaker()
        try:
            yield session
            await session.commit()
        except SQLAlchemyError as err:
            await session.rollback()
            self.logger.warning(
                "getting session",
                desc="sqlalchemy error",
                err=err
            )
            await session.invalidate()
            raise
        finally:
            await session.close()
            del session


    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[AsyncConnection, None]:
        connection = await self.engine.connect()
        try:
            yield connection
            await connection.commit()
        except Exception as err:
            await connection.rollback()
            self.logger.warning("connection error", desc="sqlalchemy error", err=str(err))
            await connection.invalidate()
            raise
        finally:
            await connection.close()
            del connection

databases = [
    SQLDatabase(
        driver=item["driver"],
        username=item["username"],
        password=item["password"],
        hostname=item["hostname"],
        port=item["port"],
        database=item["database"])

    for item in app_config.databases
]



