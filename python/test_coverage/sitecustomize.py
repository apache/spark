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

# Note that this 'sitecustomize' module is a built-in feature in Python.
# If this module is defined, it's executed when the Python session begins.
# `coverage.process_startup()` seeks if COVERAGE_PROCESS_START environment
# variable is set or not. If set, it starts to run the coverage.
try:
    import coverage
    cov = coverage.process_startup()
    if cov:
        import os

        def patch_worker():
            # If it's a worker forked from the daemon, we need to patch it to save
            # the coverage data. Otherwise the worker will be killed by a signal and
            # the coverage data will not be saved.
            import sys
            frame = sys._getframe(1)
            if (
                frame.f_code.co_name == "manager" and
                "daemon.py" in frame.f_code.co_filename and
                "worker" in frame.f_globals
            ):

                if cov := coverage.Coverage.current():
                    cov.stop()
                cov = coverage.process_startup(force=True)

                def save_when_exit(func):
                    def wrapper(*args, **kwargs):
                        try:
                            result = func(*args, **kwargs)
                        finally:
                            cov.save()
                        return result
                    return wrapper

                frame.f_globals["worker"] = save_when_exit(frame.f_globals["worker"])

        os.register_at_fork(after_in_child=patch_worker)

except ImportError:
    pass
