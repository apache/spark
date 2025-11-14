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
import faulthandler
from functools import wraps
from typing import Any, Callable

class FaultHandlerIntegration:
    def __init__(self):
        self._log_path = None
        self._log_file = None
        self._periodic_dump = False

    def start(self):
        self._log_path = os.environ.get("PYTHON_FAULTHANDLER_DIR", None)
        tracebackDumpIntervalSeconds = os.environ.get(
            "PYTHON_TRACEBACK_DUMP_INTERVAL_SECONDS", None
        )
        if self._log_path:
            self._log_path = os.path.join(self._log_path, str(os.getpid()))
            self._log_file = open(self._log_path, "w")

            faulthandler.enable(file=self._log_file)

            if tracebackDumpIntervalSeconds is not None and int(tracebackDumpIntervalSeconds) > 0:
                self._periodic_dump = True
                faulthandler.dump_traceback_later(int(tracebackDumpIntervalSeconds), repeat=True)

    def stop(self):
        if self._periodic_dump:
            faulthandler.cancel_dump_traceback_later()
            self._periodic_dump = False
        if self._log_path:
            faulthandler.disable()
            if self._log_file:
                self._log_file.close()
                self._log_file = None
            os.remove(self._log_path)
            self._log_path = None

def with_fault_handler(func: Callable):
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        fault_handler = FaultHandlerIntegration()
        try:
            fault_handler.start()
            return func(*args, **kwargs)
        finally:
            fault_handler.stop()
    return wrapper