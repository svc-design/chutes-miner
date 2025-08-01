"""
Miner API entrypoint.
"""

import os
import asyncio
import hashlib
from contextlib import asynccontextmanager
from loguru import logger
from fastapi import FastAPI, Request
from fastapi.responses import ORJSONResponse
import api.database.orms  # noqa: F401
from api.server.router import router as servers_router
from api.registry.router import router as registry_router
from api.deployment.router import router as deployments_router
from api.database import Base, engine
from api.config import settings
from api.socket_client import SocketClient


@asynccontextmanager
async def lifespan(_: FastAPI):
    """
    Execute all initialization/startup code, e.g. ensuring tables exist and such.
    """
    # SQLAlchemy init.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Lock to just one worker.
    worker_pid_file = "/tmp/api.pid"
    is_migration_process = False
    try:
        if not os.path.exists(worker_pid_file):
            with open(worker_pid_file, "x") as outfile:
                outfile.write(str(os.getpid()))
            is_migration_process = True
        else:
            with open(worker_pid_file, "r") as infile:
                designated_pid = int(infile.read().strip())
            is_migration_process = os.getpid() == designated_pid
    except FileExistsError:
        with open(worker_pid_file, "r") as infile:
            designated_pid = int(infile.read().strip())
        is_migration_process = os.getpid() == designated_pid
    if not is_migration_process:
        yield
        return

    # Manual DB migrations.
    process = await asyncio.create_subprocess_exec(
        "dbmate",
        "--url",
        settings.sqlalchemy.replace("+asyncpg", "") + "?sslmode=disable",
        "--migrations-dir",
        "api/migrations",
        "migrate",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    # Migration logging helper function.
    async def log_migrations(stream, name):
        log_method = logger.info if name == "stdout" else logger.warning
        while True:
            line = await stream.readline()
            if line:
                decoded_line = line.decode().strip()
                log_method(decoded_line)
            else:
                break

    await asyncio.gather(
        log_migrations(process.stdout, "stdout"),
        log_migrations(process.stderr, "stderr"),
        process.wait(),
    )
    if process.returncode == 0:
        logger.success("successfull applied all DB migrations")
    else:
        logger.error(f"failed to run db migrations returncode={process.returncode}")

    # Start the websocket clients.
    for validator in settings.validators:
        socket_client = SocketClient(url=validator.socket, validator=validator.hotkey)
        asyncio.create_task(socket_client.connect_and_run())

    yield


app = FastAPI(default_response_class=ORJSONResponse, lifespan=lifespan)
app.include_router(servers_router, prefix="/servers", tags=["Servers"])
app.include_router(registry_router, prefix="/registry", tags=["Registry"])
app.include_router(deployments_router, prefix="/deployments", tags=["Deployments"])
app.get("/ping")(lambda: {"message": "pong"})


@app.middleware("http")
async def request_body_checksum(request: Request, call_next):
    if request.method in ["POST", "PUT", "PATCH"]:
        body = await request.body()
        sha256_hash = hashlib.sha256(body).hexdigest()
        request.state.body_sha256 = sha256_hash
    else:
        request.state.body_sha256 = None
    return await call_next(request)
