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
import imp
import platform


# This is a hack to always refer the main code rather than built zip.
main_code_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
daemon = imp.load_source("daemon", "%s/pyspark/daemon.py" % main_code_dir)

if "COVERAGE_PROCESS_START" in os.environ:
    # PyPy with coverage makes the tests flaky, and CPython is enough for coverage report.
    if "pypy" not in platform.python_implementation().lower():
        worker = imp.load_source("worker", "%s/pyspark/worker.py" % main_code_dir)

        def _cov_wrapped(*args, **kwargs):
            import coverage
            cov = coverage.coverage(
                config_file=os.environ["COVERAGE_PROCESS_START"])
            cov.start()
            try:
                worker.main(*args, **kwargs)
            finally:
                cov.stop()
                cov.save()
        daemon.worker_main = _cov_wrapped
else:
    raise RuntimeError("COVERAGE_PROCESS_START environment variable is not set, exiting.")


if __name__ == '__main__':
    daemon.manager()
