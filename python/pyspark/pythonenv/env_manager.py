# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import logging
import shutil
import uuid
import re
from pathlib import Path
from packaging.version import Version
import functools
import subprocess
import sys
import hashlib
from pyspark import SparkFiles
import tempfile


_logger = logging.getLogger(__name__)
_IS_UNIX = os.name != "nt"


_VIRTUALENV_ENVS_DIR = "virtualenv_envs"
_PIP_CACHE_DIR = "pip_cache_pkgs"


def _get_virtualenv_extra_env_vars(env_root_dir=None):
    return {
        # PIP_NO_INPUT=1 makes pip run in non-interactive mode,
        # otherwise pip might prompt "yes or no" and ask stdin input
        "PIP_NO_INPUT": "1",
        # Specify pip cache dir
        "PIP_CACHE_DIR": os.path.join(env_root_dir, _PIP_CACHE_DIR),
    }


def _join_commands(*commands):
    entry_point = ["bash", "-c"] if _IS_UNIX else ["cmd", "/c"]
    sep = " && " if _IS_UNIX else " & "
    return [*entry_point, sep.join(map(str, commands))]


def _get_or_create_virtualenv(
        env_root_dir,
        pip_reqs,
        pip_constraints,
        capture_output=False,
):
    """
    Create an virtualenv python environment.

    :return: Command to activate the created virtualenv environment
             (e.g. "source /path/to/bin/activate").
    """
    extra_env = _get_virtualenv_extra_env_vars(env_root_dir)

    virtual_envs_root_path = Path(env_root_dir) / _VIRTUALENV_ENVS_DIR
    virtual_envs_root_path.mkdir(parents=True, exist_ok=True)

    python_bin_path = sys.executable
    env_name = _get_env_id(python_bin_path, pip_reqs, pip_constraints)
    env_dir = virtual_envs_root_path / env_name
    try:
        return _create_virtualenv(
            python_bin_path=python_bin_path,
            env_dir=env_dir,
            pip_deps=pip_reqs,
            pip_constraints=pip_constraints,
            extra_env=extra_env,
            capture_output=capture_output,
        )
    except:
        _logger.warning("Encountered unexpected error while creating %s", env_dir)
        if env_dir.exists():
            _logger.warning("Attempting to remove %s", env_dir)
            shutil.rmtree(env_dir, ignore_errors=True)
            msg = "Failed to remove %s" if env_dir.exists() else "Successfully removed %s"
            _logger.warning(msg, env_dir)
        raise


def _get_env_id(python_bin_path, pip_reqs, pip_constraints):
    """
    Create python environment id based on python bin path, sorted pip requirement list,
    and sorted pip constraint list.
    """
    env_str = f"{python_bin_path}-{','.join(sorted(pip_reqs))}-{'.'.join(sorted(pip_constraints))}"
    return hashlib.sha1(env_str.encode("utf-8")).hexdigest()


def _create_virtualenv(
        python_bin_path,
        env_dir,
        pip_deps,
        pip_constraints,
        extra_env=None,
        capture_output=False
):
    # Created a command to activate the environment
    paths = ("bin", "activate") if _IS_UNIX else ("Scripts", "activate.bat")
    activate_cmd = env_dir.joinpath(*paths)
    activate_cmd = f"source {activate_cmd}" if _IS_UNIX else activate_cmd

    if env_dir.exists():
        _logger.info("Environment %s already exists", env_dir)
        return activate_cmd

    _logger.info("Creating a new environment in %s with %s", env_dir, python_bin_path)
    _exec_cmd(["virtualenv", "--python", python_bin_path, env_dir], capture_output=capture_output)

    _logger.info("Installing dependencies")
    with tempfile.NamedTemporaryFile(SparkFiles.getRootDirectory()) as temp_dir:
        # prepare pip requirements file
        req_file_path = os.path.join(temp_dir, "requirements.txt")
        constraints_file_path = os.path.join(temp_dir, "constraints.txt")

        with open(req_file_path, "w") as f:
            f.writelines(pip_deps)
            f.write(f"-c {constraints_file_path}")

        with open(constraints_file_path, "w") as f:
            f.writelines(pip_constraints)

        cmd = _join_commands(
            activate_cmd, f"python -m pip install --quiet -r {req_file_path}"
        )
        _exec_cmd(cmd, capture_output=capture_output, cwd=temp_dir, extra_env=extra_env)

    return activate_cmd


class Environment:
    def __init__(self, activate_cmd, extra_env=None):
        if not isinstance(activate_cmd, list):
            activate_cmd = [activate_cmd]
        self._activate_cmd = activate_cmd
        self._extra_env = extra_env or {}

    def get_activate_command(self):
        return self._activate_cmd

    def execute(
            self,
            command,
            command_env=None,
            preexec_fn=None,
            capture_output=False,
            stdout=None,
            stderr=None,
            stdin=None,
            synchronous=True,
    ):
        if command_env is None:
            command_env = os.environ.copy()
        command_env = {**self._extra_env, **command_env}
        if not isinstance(command, list):
            command = [command]

        if _IS_UNIX:
            separator = " && "
        else:
            separator = " & "

        command = separator.join(map(str, self._activate_cmd + command))
        if _IS_UNIX:
            command = ["bash", "-c", command]
        else:
            command = ["cmd", "/c", command]
        _logger.info("=== Running command '%s'", command)
        return _exec_cmd(
            command,
            env=command_env,
            capture_output=capture_output,
            synchronous=synchronous,
            preexec_fn=preexec_fn,
            close_fds=True,
            stdout=stdout,
            stderr=stderr,
            stdin=stdin,
        )


def _exec_cmd(
        cmd,
        *,
        throw_on_error=True,
        extra_env=None,
        capture_output=True,
        synchronous=True,
        stream_output=False,
        **kwargs,
):
    """
    A convenience wrapper of `subprocess.Popen` for running a command from a Python script.

    :param cmd: The command to run, as a string or a list of strings
    :param throw_on_error: If True, raises an Exception if the exit code of the program is nonzero.
    :param extra_env: Extra environment variables to be defined when running the child process.
                      If this argument is specified, `kwargs` cannot contain `env`.
    :param capture_output: If True, stdout and stderr will be captured and included in an exception
                           message on failure; if False, these streams won't be captured.
    :param synchronous: If True, wait for the command to complete and return a CompletedProcess
                        instance, If False, does not wait for the command to complete and return
                        a Popen instance, and ignore the `throw_on_error` argument.
    :param stream_output: If True, stream the command's stdout and stderr to `sys.stdout`
                          as a unified stream during execution.
                          If False, do not stream the command's stdout and stderr to `sys.stdout`.
    :param kwargs: Keyword arguments (except `text`) passed to `subprocess.Popen`.
    :return:  If synchronous is True, return a `subprocess.CompletedProcess` instance,
              otherwise return a Popen instance.
    """
    illegal_kwargs = set(kwargs.keys()).intersection({"text"})
    if illegal_kwargs:
        raise ValueError(f"`kwargs` cannot contain {list(illegal_kwargs)}")

    env = kwargs.pop("env", None)
    if extra_env is not None and env is not None:
        raise ValueError("`extra_env` and `env` cannot be used at the same time")

    if capture_output and stream_output:
        raise ValueError(
            "`capture_output=True` and `stream_output=True` cannot be specified at the same time"
        )

    env = env if extra_env is None else {**os.environ, **extra_env}

    # In Python < 3.8, `subprocess.Popen` doesn't accept a command containing path-like
    # objects (e.g. `["ls", pathlib.Path("abc")]`) on Windows. To avoid this issue,
    # stringify all elements in `cmd`. Note `str(pathlib.Path("abc"))` returns 'abc'.
    if isinstance(cmd, list):
        cmd = list(map(str, cmd))

    if capture_output or stream_output:
        if kwargs.get("stdout") is not None or kwargs.get("stderr") is not None:
            raise ValueError(
                "stdout and stderr arguments may not be used with capture_output or stream_output"
            )
        kwargs["stdout"] = subprocess.PIPE
        if capture_output:
            kwargs["stderr"] = subprocess.PIPE
        elif stream_output:
            # Redirect stderr to stdout in order to combine the streams for unified printing to
            # `sys.stdout`, as documented in
            # https://docs.python.org/3/library/subprocess.html#subprocess.run
            kwargs["stderr"] = subprocess.STDOUT

    process = subprocess.Popen(
        cmd,
        env=env,
        text=True,
        **kwargs,
    )
    if not synchronous:
        return process

    if stream_output:
        for output_char in iter(lambda: process.stdout.read(1), ""):
            sys.stdout.write(output_char)

    stdout, stderr = process.communicate()
    returncode = process.poll()
    comp_process = subprocess.CompletedProcess(
        process.args,
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )
    if throw_on_error and returncode != 0:
        raise ShellCommandException.from_completed_process(comp_process)
    return comp_process


class ShellCommandException(Exception):
    @classmethod
    def from_completed_process(cls, process):
        lines = [
            f"Non-zero exit code: {process.returncode}",
            f"Command: {process.args}",
        ]
        if process.stdout:
            lines += [
                "",
                "STDOUT:",
                process.stdout,
            ]
        if process.stderr:
            lines += [
                "",
                "STDERR:",
                process.stderr,
            ]
        return cls("\n".join(lines))
