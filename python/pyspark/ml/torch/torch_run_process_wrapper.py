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
import signal
import subprocess
import sys
import threading
import time
from typing import Any


def clean_and_terminate(task: "subprocess.Popen") -> None:
    task.terminate()
    time.sleep(0.5)
    if task.poll() is None:
        task.kill()
    # TODO(SPARK-41775): Cleanup temp files


def check_parent_alive(task: "subprocess.Popen") -> None:
    orig_parent_id = os.getppid()
    while True:
        if os.getppid() != orig_parent_id:
            clean_and_terminate(task)
            break
        time.sleep(0.5)


if __name__ == "__main__":
    """
    This is a wrapper around torch.distributed.run and it kills the child process
    if the parent process fails, crashes, or exits.
    """

    args = sys.argv[1:]

    cmd = [sys.executable, "-m", "torch.distributed.run", *args]
    task = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE,
        env=os.environ,
    )
    t = threading.Thread(target=check_parent_alive, args=(task,), daemon=True)

    def sigterm_handler(*args: Any) -> None:
        clean_and_terminate(task)
        os._exit(0)

    signal.signal(signal.SIGTERM, sigterm_handler)

    t.start()
    task.stdin.close()  # type: ignore[union-attr]
    try:
        for line in task.stdout:  # type: ignore[union-attr]
            decoded = line.decode()
            print(decoded.rstrip())
        task.wait()
    finally:
        if task.poll() is None:
            try:
                task.terminate()
                time.sleep(0.5)
                if task.poll() is None:
                    task.kill()
            except OSError:
                pass
