from __future__ import annotations

import os
import pwd
import sys
import json
from pathlib import Path
import shutil
import subprocess
import jinja2
from textwrap import dedent
import inspect
import pickle

from api.snowpark_helpers import (
    _try_parse_snowflake_xcom_uri,
    _is_table_arg,
    _deserialize_snowpark_args,
    _write_snowpark_dataframe,
    _serialize_snowpark_results
)


class PythonProjectEnv(object):

    def __init__(
        self, 
        run_dir: Path, 
        project_prefix: str,
        payload: dict,
        log_file: str | None = None
    ):
        self.payload: dict = payload
        self.project_prefix: str = project_prefix
        self.project_dir: Path = run_dir / project_prefix
        self.payload_file: Path = self.project_dir / 'payload.json'
        self.log_file: str | None = log_file

    def __enter__(self):

        print("Created user directory.")
        assert not self.project_dir.exists(), f"Project prefix collision. Retry task."
        self.project_dir.mkdir()
        
        self.payload_file.write_text(json.dumps(self.payload))

        print("Creating a python virtual environment.")
        self.python_exe = self.payload.get(Path(f"/usr/local/bin/python{self.payload['python_version']}"),
                                           Path(sys.executable))
        assert self.python_exe.is_absolute() and self.python_exe.exists(), f"User provided python {self.payload['python_version']} not available on runner.  Specify maj.min version only."

        virtualenv_cmd = [self.python_exe.as_posix(), "-m", "virtualenv", self.project_dir.as_posix()]

        if self.payload['system_site_packages']:
            virtualenv_cmd.append("--system-site-packages")

        proc = subprocess.check_call(virtualenv_cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE) #, log_file=self.log_file)

        self.user_python = self.project_dir.joinpath('bin/python')
        
        if self.payload['requirements']:
            print("Installing requirements")
            pip_cmd = [self.user_python, "-m", "pip", "install", "--cache-dir=/tmp/pip_cache"] \
                + self.payload.get('pip_install_options') \
                + self.payload['requirements']
            
            proc = subprocess.check_call(pip_cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE) #, log_file=self.log_file)

        self.input_path = self.project_dir.joinpath("script.in")
        if self.payload['op_args'] or self.payload['op_kwargs']:
            print("Writing args to file.")
            self.input_path.write_bytes(pickle.dumps({"args": self.payload['op_args'], "kwargs": self.payload['op_kwargs']}))

        self.string_args_path = self.project_dir.joinpath("string_args.txt")
        self.string_args_path.write_text("\n".join(map(str, self.payload['string_args'])))
        
        print("Writing jinja templated executable.")
        self.script_path = self.project_dir.joinpath("script.py")
        jinja_context=dict(
            conn_params = self.payload['snowflake_user_conn_params'],
            log_level = self.payload['log_level'],
            temp_data_dict = self.payload['temp_data_dict'],
            dag_id = self.payload['dag_id'],
            task_id = self.payload['task_id'],
            run_id = self.payload['run_id'],
            ts_nodash=self.payload['ts_nodash'],
            op_args = self.payload['op_args'],
            op_kwargs = self.payload['op_kwargs'],
            expect_airflow = False,
            pickling_library = 'pickle',
            python_callable = self.payload['python_callable_name'],
            python_callable_source = self.payload['python_callable_str'],
        )
                
        template_loader = jinja2.FileSystemLoader(searchpath=os.path.dirname(__file__))
        template_env: jinja2.Environment
        template_env = jinja2.Environment(loader=template_loader, undefined=jinja2.StrictUndefined)
        template = template_env.get_template("snowpark_virtualenv_script.jinja2")
        template.stream(**jinja_context).dump(self.script_path.as_posix())

        # send_message(MessageType.EXEC, f"Creating user account {self.project_prefix} to run the task.", log_file=self.log_file)
        try:
            subprocess.check_call(["useradd", "-d", self.project_dir.as_posix(), self.project_prefix], stderr=subprocess.STDOUT, stdout=subprocess.PIPE) #, log_file=self.log_file)
        except subprocess.CalledProcessError:  # User exists
            raise Exception('Project user exists. Project prefix collision. Retry task.')

        uid = pwd.getpwnam(self.project_prefix).pw_uid
        gid = pwd.getpwnam(self.project_prefix).pw_gid
        
        for root, dirs, files in os.walk(self.project_dir.as_posix()):  
            os.chown(root, uid=uid, gid=gid)
            for file in files:
                os.chown(os.path.join(root, file), uid=uid, gid=gid)

        self.output_path = self.project_dir.joinpath("script.out")

        self.cmd=[
                    self.user_python.as_posix(),
                    self.script_path.as_posix(),
                    self.input_path.as_posix(),
                    self.output_path.as_posix(),
                    self.string_args_path.as_posix(),
                ]
        
        return self

    def __exit__(self, type, value, traceback):
        subprocess.check_call(["userdel", self.project_prefix]) #, log_file=self.log_file)
        # shutil.rmtree(self.project_dir.as_posix())
