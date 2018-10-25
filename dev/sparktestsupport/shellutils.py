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

from __future__ import print_function
import os
import shutil
import subprocess
import sys

subprocess_check_output = subprocess.check_output
subprocess_check_call = subprocess.check_call


def exit_from_command_with_retcode(cmd, retcode):
    if retcode < 0:
        print("[error] running", ' '.join(cmd), "; process was terminated by signal", -retcode)
    else:
        print("[error] running", ' '.join(cmd), "; received return code", retcode)
    sys.exit(int(os.environ.get("CURRENT_BLOCK", 255)))


def rm_r(path):
    """
    Given an arbitrary path, properly remove it with the correct Python construct if it exists.
    From: http://stackoverflow.com/a/9559881
    """

    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.exists(path):
        os.remove(path)


def run_cmd(cmd, return_output=False):
    """
    Given a command as a list of arguments will attempt to execute the command
    and, on failure, print an error message and exit.
    """

    if not isinstance(cmd, list):
        cmd = cmd.split()
    try:
        if return_output:
            return subprocess_check_output(cmd)
        else:
            return subprocess_check_call(cmd)
    except subprocess.CalledProcessError as e:
        exit_from_command_with_retcode(e.cmd, e.returncode)


def is_exe(path):
    """
    Check if a given path is an executable file.
    From: http://stackoverflow.com/a/377028
    """

    return os.path.isfile(path) and os.access(path, os.X_OK)


def which(program):
    """
    Find and return the given program by its absolute path or 'None' if the program cannot be found.
    From: http://stackoverflow.com/a/377028
    """

    fpath = os.path.split(program)[0]

    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ.get("PATH").split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None
