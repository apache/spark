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

HAS_CHECK_DEBUGGER = False


def check_debugger():
    global HAS_CHECK_DEBUGGER

    if HAS_CHECK_DEBUGGER:
        return

    HAS_CHECK_DEBUGGER = True
    env = dict(os.environ)
    debug_server = env.get('PYTHON_REMOTE_DEBUG_SERVER')
    debug_port = env.get('PYTHON_REMOTE_DEBUG_PORT')

    if debug_server is not None and debug_port is not None:
        try:
            import pydevd
            print('connecting debug server %s, port %s'
                  % (debug_server, debug_port))
            pydevd.settrace(debug_server,
                            port=int(debug_port),
                            stdoutToServer=False,
                            stderrToServer=False)
        except Exception as e:
            print(e)
            raise Exception('init debugger fail.')
