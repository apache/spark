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

import io
import sys

PY3 = sys.version_info > (3,)

_have_numpy = False

try:
    import numpy as np
    _have_numpy = True
except:
    # No NumPy 
    pass

def numpy_to_python(convertable):
    if not _have_numpy:
        raise RuntimeError('Numpy is not available')

    if isinstance(convertable, np.generic):
        return np.asscalar(convertable)
    elif isinstance(convertable, list):
        return [numpy_to_python(elem) for elem in convertable]
    elif isinstance(convertable, tuple):
        return tuple([numpy_to_python(elem) for elem in convertable])
    elif isinstance(convertable, np.ndarray):
        output = io.BytesIO()
        np.save(output, arr=convertable)
        output.seek(0)

        if PY3:
            return bytes(output.read())
        else:
            return bytearray(output.read())
    else:
        return convertable


def python_to_numpy(convertable):
    if not _have_numpy:
        raise RuntimeError('Numpy is not available')

    if isinstance(convertable, list):
        return [python_to_numpy(elem) for elem in convertable]
    elif isinstance(convertable, tuple):
        return tuple([python_to_numpy(elem) for elem in convertable])
    elif (PY3 and isinstance(convertable, bytes)) or isinstance(convertable, bytearray):
        input_bytes = io.BytesIO(convertable)
        return np.load(input_bytes)
    else:
        return convertable
