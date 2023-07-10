from snowflake.snowpark import Session as SnowparkSession
import json
import pickle
from pathlib import Path
from uuid import uuid4
import subprocess
from api.log import log, MessageType
from api.setup_python import PythonProjectEnv, execute_in_subprocess

run_dir = Path('/run')
project_prefix = f'{uuid4()}'.replace('-','').upper()
log_file = run_dir / project_prefix / "logs.txt"

payload = json.loads(Path('/xcom/payload.json').read_text())
# payload['requirements']=['pandas', 'numpy']
# payload['python_callable_name']='myfunc1'
# payload['python_callable_str'] = """def myfunc1():
#     import pandas as pd
#     df = pd.DataFrame([{"a": 1, "b": 1}, {"a": 1, "b": 1}, {"a": 1, "b": 1}])
#     a=1
#     b=1
#     #c
#     print('stuff')
#     print('morestuff')
#     return (df.to_json(), df)
# """

response = SnowparkSession.builder.configs(payload['snowflake_user_conn_params']).create().sql('select 1').collect()
assert str(response) == '[Row(1=1)]'

with PythonProjectEnv(run_dir=run_dir, project_prefix=project_prefix, payload=payload, log_file=log_file) as env:

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
            log(MessageType.OUTPUT, outs.decode('utf-8'))
        if errs:
            log(MessageType.ERROR, errs.decode('utf-8'))

    #process return results
    with open(env.output_path, 'rb') as file:
        ret_val = pickle.load(file)

    try:
        parsed = json.dumps(ret_val)
        log(MessageType.RESULT, ret_val)
    except:
        log(MessageType.ERROR, f"Cannot return data type {str(type(ret_val))}")


