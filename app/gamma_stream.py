import pandas as pd
import asyncio
import sqlalchemy.exc
from confluent_kafka import KafkaException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.testing.util import gc_collect
from structlog import get_logger

from src.services.kafka_service import KafkaSuite
from src.models.events_model import Events
from src.models.app_config import app_config
from src.services.database_service import databases, SQLDatabase
from pydantic import BaseModel
from decimal import Decimal
from initiate import initiate_pipeline
from traceback import format_exc
from contextlib import AsyncExitStack

import gc

MAX_RETRIES = 3
BACKOFF = 5


pd.set_option("future.no_silent_downcasting", True)

class GammaStream:

    def __init__(self, worker_id: int) -> None:
        self.logger = get_logger()
        self.name = app_config.stream_name
        self.databases: list[SQLDatabase] = databases
        self.worker_id: int = worker_id
        self.logger_metadata = f"{self.name} - [{str(self.worker_id)}]"
        self.kafka_suite = KafkaSuite(partition_id=self.worker_id)
        self.speed = 0
        self.events = Events(
            pause=asyncio.Event(),
            resume=asyncio.Event(),
            export=asyncio.Event(),
            clear_memory=asyncio.Event(),
            slowdown=asyncio.Event(),
            speedup=asyncio.Event(),
            kill=asyncio.Event()
        )


    async def start_streaming(self) -> None:
        self.logger.info(self.logger_metadata, action="streaming...")
        while True:
            try:
                if self.events.pause.is_set():
                    self.kafka_suite.pause(worker_id=self.worker_id)
                    self.events.resume.clear()
                    self.logger.info(self.logger_metadata, action="paused externally")
                    await self.events.resume.wait()
                    self.kafka_suite.resume(worker_id=self.worker_id)
                    self.events.resume.clear()
                    self.events.pause.clear()
                    self.logger.info(self.logger_metadata, action="resumed externally")

                if self.events.clear_memory.is_set():
                    gc_collect()
                    self.events.clear_memory.clear()
                    self.logger.info(self.logger_metadata, action="memory cleared")
                if self.events.kill.is_set():
                    raise asyncio.CancelledError("killed externally")

                key, value = await self.kafka_suite.get_message()
                if key is None and value is None:
                    continue

                self.logger.info(self.logger_metadata, action="message received")
                async with AsyncExitStack() as stack:
                    conns = [await stack.enter_async_context(database.get_connection()) for database in self.databases]
                    self.logger.debug(self.logger_metadata, action="connections created")
                    await self.process_payload(key=key, value=value, connections=conns)
                    
            except sqlalchemy.exc.OperationalError as err:
                self.logger.warning(self.logger_metadata, err=err, action="trying to reconnect...", tb=format_exc()
                )
                for database in self.databases:
                    await database.reconnect(max_retries=MAX_RETRIES, backoff=BACKOFF)
            except asyncio.CancelledError as err:
                self.logger.warning(self.logger_metadata, action="initiating graceful shutdown", err=err)
                await self.graceful_shutdown()
                return
            except (
                    TypeError,
                    ValueError,
                    SQLAlchemyError,
                    KafkaException,
                    ValueError
            )  as err:
                self.logger.error(self.logger_metadata, err=err, tb=format_exc())

    async def process_payload(self, key: dict, value: dict, connections: list[AsyncConnection]) -> None:
        self.logger.info(self.logger_metadata, action="processing payload", key=key, op=value["op"])
        try:
            if value["op"] not in {"u", "c", "d"}:
                return
            dataset_df = await initiate_pipeline(self=self, key=key, value=value, connections=connections)
            if dataset_df is None:
                return
            self.kafka_suite.deploy_success_payload(
                key=str(key),
                value=dataset_df
            )
            if self.events.slowdown.is_set():
                self.events.slowdown.clear()
                self.speed += 0.5
            if self.events.speedup.is_set():
                self.events.speedup.clear()
                if self.speed > 0:
                    self.speed -= 0.5
                else:
                    self.logger.info("Couldn't speed up any further")
            await asyncio.sleep(self.speed)
        except SQLAlchemyError as err:
            self.logger.warning(self.logger_metadata, action="operational error raised", err=err)
            self.kafka_suite.deploy_fail_payload(key=key, value=value)
        except KeyError as err:
            self.logger.warning(self.logger_metadata, err=err)
            self.kafka_suite.deploy_fail_payload(key=key, value=value)
        except Exception as err:
            self.logger.warning(
                event="coroutine finished with error",
                err=err
            )
            self.kafka_suite.deploy_fail_payload(key=key, value=value)
            raise

    def find_connection(self, connections: list[AsyncConnection], conn_name: str) -> AsyncConnection:
        try:
            connection = next(connection for connection in connections if connection.engine.url.database == conn_name)
            return connection
        except StopIteration as err:
            self.logger.info(
                self.logger_metadata,
                "error in getting connection",
                err=err,
                desc=f"connection {conn_name} not found",
                current_connections=str(connections)
            )
            raise

    def convert_to_boolean(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        for column_name in columns:
            if pd.api.types.is_integer_dtype(df[column_name]):
                df[column_name] = df[column_name].apply(lambda x: True if x == 1 else False)
            if df[column_name].isna().all():
                df[column_name] = False
        return df

    def parse_type(self, dataframe: pd.DataFrame, col: str, field: dict) -> pd.DataFrame:
        field_type = field["type"]
        if isinstance(field_type, list):
            non_null_type = [t for t in field_type if t != "null"][0]
            if isinstance(non_null_type, dict):
                non_null_type = non_null_type.get("logicalType")
            match non_null_type:
                case "int":
                    dataframe[col] = dataframe[col].astype("Int64")
                case "boolean":
                    dataframe[col] = dataframe[col].astype("boolean")  ## ???
                case "string":
                    dataframe[col] = dataframe[col].astype("str")  ## ???
                case "timestamp-millis":
                    try:
                        if pd.api.types.is_datetime64_dtype(dataframe[col]):
                            # dataframe[col].iloc[0].to_pydatetime()
                            pass
                        elif dataframe[col].isna().all():
                            dataframe[col] = dataframe[col].astype("object").apply(lambda x: None)
                        elif pd.api.types.is_object_dtype(dataframe[col]):
                            try:
                                dataframe[col] = pd.to_datetime(dataframe[col])
                            except Exception:
                                pass
                        else:
                            raise ValueError(f"type of column {col} not recognized, {dataframe[col].dtype}")
                    except ValueError as err:
                        self.logger.warning(self.logger_metadata, action="malformed timestamp column", err=err)
                        raise err
                case "decimal":
                    if isinstance(dataframe[col].iloc[0], Decimal):
                        pass
                    elif dataframe[col].isna().all():
                        dataframe[col] = dataframe[col].astype("object").apply(lambda x: None)
                    else:
                        if isinstance(non_null_type, dict):
                            precision = non_null_type.get("precision")
                            scale = non_null_type.get("scale")
                            dataframe[col] = dataframe[col].apply(lambda x: Decimal(x) / Decimal(precision ** scale)) # Decimal(19000.00) ? scale = 2, precision=10
                        else:
                            self.logger.warning(self.logger_metadata, action=f"something weird {non_null_type}")
                case "float":
                    if dataframe[col].isna().all():
                        dataframe[col] = dataframe[col].astype("object").apply(lambda x: None)
                    else:
                        dataframe[col] = dataframe[col].astype(float)
                case _:
                    raise ValueError(f"Unknown type, dtype={dataframe[col].dtype}, field_type={field_type},col={col}")
        elif type(field_type) == str:
            dataframe[col] = dataframe[col].astype("Int64")
        else:
            raise ValueError("AVRO schema contains more than one allowable type, next to null. MENJAJ")
        return dataframe

    async def graceful_shutdown(self) -> None:
        try:
            self.kafka_suite.close()
            self.logger.info("kafka suite closed successfully")
        except Exception as err:
            self.logger.error("error closing Kafka suite", err=err)
        try:
            for database in self.databases:
                await database.engine.dispose(close=True)
                self.logger.info(f"engine for {database.database} has been disposed")
        except asyncio.CancelledError:
            pass
        except Exception as err:
            self.logger.error(f"error disposing engine for {database.database}", err=err)

        # Optionally collect garbage
        gc.collect()
        self.logger.info("garbage collection complete")
        self.logger.warning("graceful shutdown finished")


    async def get_sql_results(self, sql_query: str, connection: AsyncConnection) -> pd.DataFrame:
        try:
            result = await connection.execute(text(sql_query))
            pd_result = pd.DataFrame(result)
        except Exception as err:
            self.logger.warning(
                self.logger_metadata,
                action=f"exception raised by {connection.engine.name}",
                sql=sql_query,
                err=err
            )
            raise
        else:
            del result
            return pd_result

    async def begin_querying(
            self,
            connections: list[AsyncConnection],
            custom_id_column_name: str = app_config.kafka_payload_key,
            **tasks_groups: dict):

        prepared_tasks = dict()
        final_results = dict()
        try:
            async with asyncio.TaskGroup() as asyncio_context:
                for group_name, group_tasks in tasks_groups.items():
                    db_conn = self.find_connection(connections=connections, conn_name=group_name)
                    asyncio_tasks = {
                        sql_name: asyncio_context.create_task(self.get_sql_results(sql, db_conn))
                        for sql_name, sql in group_tasks.items()
                    }
                    prepared_tasks.update({group_name: asyncio_tasks})

            for group_name, prepared_tasks in prepared_tasks.items():
                pandified = {
                    sql_name+'_df': asyncio_task.result() for sql_name, asyncio_task in prepared_tasks.items()
                }
                final_results.update({group_name: pandified})

            # make sure all id types are nullable
            for group_name, dataframe_tasks in final_results.items():
                for sql_task, dataframe in dataframe_tasks.items():
                    dataframe_tasks[sql_task][custom_id_column_name] = \
                        dataframe[custom_id_column_name].astype("Int64")
        except KeyError as err:
            self.logger.warning(self.logger_metadata, action="record doesn't exist in the database", err=err)
            raise
        except Exception as err:
            self.logger.warning(self.logger_metadata, err=err)
            raise
        self.logger.info(self.logger_metadata, action="querying sucessful, loading data into pd.Dataframe")
        return final_results

from typing import Any

class StreamModel(BaseModel):
    stream_instance: GammaStream
    coroutine: asyncio.Task

    class Config:
        arbitrary_types_allowed = True