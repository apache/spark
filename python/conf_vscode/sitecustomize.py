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

try:
    import os
    import sys

    if (
        "DEBUGPY_ADAPTER_ENDPOINTS" in os.environ and
        not any("debugpy" in arg for arg in sys.orig_argv)
    ):

        def install_debugpy():
            import debugpy
            import fcntl

            lock_file = os.getenv("DEBUGPY_ADAPTER_ENDPOINTS") + ".lock"
            try:
                fd = os.open(lock_file, os.O_CREAT | os.O_RDWR, 0o600)
                fcntl.flock(fd, fcntl.LOCK_EX)
                debugpy.listen(0)
                debugpy.wait_for_client()
                try:
                    os.remove(os.getenv("DEBUGPY_ADAPTER_ENDPOINTS"))
                except Exception:
                    pass
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)

        if "pyspark.daemon" in sys.orig_argv and "PYSPARK_DEBUGPY_HOOK_DAEMON" not in os.environ:
            # Hooking daemon could be potentially dangerous because we need to fork later
            os.register_at_fork(after_in_child=install_debugpy)
        else:
            install_debugpy()

except ImportError:
    pass
