import asyncio
import json
import sys
import gc
import fastapi
import uvicorn

from contextlib import asynccontextmanager
from src.models.app_config import app_config
from inspira_stream import InspiraStream, StreamModel
from src.services.database_service import databases
from src.logging_setup.custom_logger import configure_logging
from structlog import get_logger
from concurrent.futures import ThreadPoolExecutor
from fastapi.middleware.cors import CORSMiddleware
from traceback import format_exc

import sentry_sdk


configure_logging()
logger = get_logger()


# sentry_sdk.init(
#     "<sentry project link>",
#
#     # Set traces_sample_rate to 1.0 to capture 100%
#     # of transactions for performance monitoring.
#     # We recommend adjusting this value in production.
#     traces_sample_rate=1.0
# )


stream: asyncio.Task
workers: dict[int, StreamModel] = {}
executor = ThreadPoolExecutor(max_workers=2 * app_config.number_of_coroutines)


async def kill_workers():
    global workers
    for worker_id, worker in workers.items():
        try:
            worker.coroutine.cancel("killing workers")
            await worker.coroutine
        except asyncio.CancelledError:
            logger.info(f"worker {worker_id} was successfully cancelled.")
        except Exception as err:
            logger.warning(f"failed to cancel worker {worker_id}: {err}", tb=format_exc())
    workers.clear()


async def create_streams():
    """connect to databases and start worker tasks"""
    global workers
    try:
        logger.info("databases list", list=[database.database for database in databases])
        await asyncio.gather(*[database.connect() for database in databases])  # Ensure all databases connect
        logger.info("databases connected")

        async with asyncio.TaskGroup() as main_group:
            for worker_id in range(app_config.number_of_coroutines):
                stream = InspiraStream(worker_id=worker_id)
                task = main_group.create_task(stream.start_streaming())
                workers[worker_id] = StreamModel(stream_instance=stream, coroutine=task)
            logger.info("Workers started", number=app_config.number_of_coroutines)
        return [scheduled_task.coroutine.result() for scheduled_task in workers.values()]

    except ExceptionGroup as err:
        logger.error("taskgroup finished with an error", tb=format_exc(), err=err)
        await kill_workers()
        raise


async def begin():
    """Run in perpetuum, handling errors and managing workers"""
    global workers
    global executor
    loop = asyncio.get_running_loop()
    loop.set_task_factory(asyncio.eager_task_factory)
    loop.set_default_executor(executor)
    try:
        await create_streams()
    except Exception as err:
        logger.critical("running streams failed", err=err, tb=format_exc())
    return

@asynccontextmanager
async def lifespan(app: fastapi.FastAPI):
    global stream
    logger.info("start")
    stream = asyncio.create_task(begin())
    try:
        yield
    except Exception as err:
        logger.error("Error during FastAPI lifespan", err=err)
    finally:
        logger.critical("shutting down the application", action="cancelling begin() coroutine")
        stream.cancel(msg="lifespan of FastAPI instance ended")
        try:
            await kill_workers()
        except Exception as err:
            pass
        workers.clear()
        await asyncio.get_event_loop().shutdown_default_executor()
        logger.critical("shutting down the application", action="default executor killed")
        gc.collect()



app = fastapi.FastAPI(lifespan=lifespan)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/pause_all_workers")
async def pause_all_workers():
    for worker_id in range(app_config.number_of_coroutines):
        workers[worker_id].stream_instance.events.pause.set()
    return fastapi.Response(status_code=200)

@app.post("/resume_all_workers")
async def resume_all_workers():
    for worker_id in range(app_config.number_of_coroutines):
        workers[worker_id].stream_instance.events.resume.set()
    return fastapi.Response(status_code=200)

@app.post("/slowdown")
async def slowdown():
    for worker in workers.values():
        worker.stream_instance.events.slowdown.set()
    return fastapi.Response(status_code=200)

@app.post("/speedup")
async def slowdown():
    for worker in workers.values():
        worker.stream_instance.events.speedup.set()
    return fastapi.Response(status_code=200)

@app.post("/status")
async def status():
    status = {}
    for worker in workers.values():
        status.update({
          f"{worker.stream_instance.worker_id}": {
                "pause" : worker.stream_instance.events.pause.is_set(),
                "slowdown" : worker.stream_instance.speed
          }})
    return fastapi.Response(status_code=200, content=json.dumps(status))

if __name__ == '__main__':
    uvicorn.run("main:app", host="0.0.0.0", port=50000, workers=1)
    logger.critical("end")
