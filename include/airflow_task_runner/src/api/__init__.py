import os
from uuid import uuid4
from pathlib import Path
import asyncio
import subprocess
import traceback
import json
import pickle
from prometheus_fastapi_instrumentator import Instrumentator

 
from dateutil import parser as dp

from fastapi import FastAPI, WebSocket
# from fastapi.middleware.cors import CORSMiddleware

# import sentry_sdk
# from sentry_sdk.integrations.asgi import SentryAsgiMiddleware

from snowflake.snowpark import Session as SnowparkSession

from api.setup_python import PythonProjectEnv
from api.log import log, MessageType
from api.snowpark_helpers import SnowparkTable

# # Set up sentry
# try:
#     sentry_sdk.init(
#         dsn=os.getenv("SENTRY_DSN", ""),
#         environment=os.getenv("ENVIRONMENT", "local"),
#         traces_sample_rate=1.0,
#     )
# except:
#     pass


app = FastAPI(
    title="Example Airflow Runner for Snowpark Containers API",
    description="Snowpark Containers Runner API",
    version="0.1.0",
)

Instrumentator().instrument(app).expose(app)

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# app.add_middleware(
#     SentryAsgiMiddleware,
# )
 
# app.include_router(task_results_router)
# app.include_router(test_connection_router)

@app.get("/", response_model=str)
async def ping() -> str:
    return "pong"

@app.websocket("/task")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """
    Listens for connection with payload of python function to execute in Airflow.

    Expected payload: 
    Python payload = dict(
        python_callable_str:str,
        python_callable_name:str
        snowflake_user_conn_params:dict
        requirements:list|None
        pip_install_options:list|None
        temp_data_dict:dict,
        system_site_packages:bool,
        python_version:str|None,
        dag_id:str,
        task_id:str,
        run_id:str,
        ts_nodash:str,
        op_args:list|None,
        op_kwargs:dict|None,
        string_args:str|None,
    )
    """

    await websocket.accept()

    try:
        # Receive payload with Task Run config
        payload:str = await websocket.receive_json()

        run_dir = Path('/run')
        project_prefix = f'{uuid4()}'.replace('-','').upper()
        log_file = run_dir / project_prefix / "logs.txt"

        asyncio.create_task(websocket.send_json(log(MessageType.EXEC, 'Checking user access with Snowflake.')))
        
        #check auth.  If the user can't authenticate to Snowflake we won't accept the run.
        response = SnowparkSession.builder.configs(payload['snowflake_user_conn_params']).create().sql('select 1').collect()

        if str(response) != '[Row(1=1)]':
            await asyncio.create_task(websocket.send_json(json.dumps(log(MessageType.ERROR, response[1]))))
            return
            
        elif payload.get('python_callable_str'):

            with PythonProjectEnv(run_dir=run_dir, project_prefix=project_prefix, payload=payload, log_file=log_file) as env:

                asyncio.create_task(websocket.send_json(log(MessageType.EXEC, 'Running python callable.')))

                proc: subprocess.Popen = subprocess.Popen(
                    ["/usr/local/bin/python3.9", "-W", "ignore", "/app/api/run_python_task.py", json.dumps(env.cmd)],
                    user=env.project_prefix,
                    cwd=env.project_dir.as_posix(),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

                while proc.poll() is None:
                    outs, errs = proc.communicate()
                    if outs:
                        await asyncio.create_task(websocket.send_json(log(MessageType.OUTPUT, outs.decode('utf-8'))))
                    if errs:
                        await asyncio.create_task(websocket.send_json(log(MessageType.ERROR, errs.decode('utf-8'))))
                
                #process return results
                with open(env.output_path, 'rb') as file:
                    ret_val = pickle.load(file)

                try:
                    parsed = json.dumps(ret_val)
                    await asyncio.create_task(websocket.send_json(log(MessageType.RESULT, parsed)))
                except:
                    await asyncio.create_task(websocket.send_json(log(MessageType.ERROR, f"Cannot return data type {str(type(ret_val))}")))

    except Exception:
        await asyncio.create_task(websocket.send_json(log(MessageType.ERROR, traceback.format_exc())))

    finally:
        await websocket.close()