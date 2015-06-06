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

import array

_have_numpy = False
_have_pandas = False

try:
    import numpy as np
    _have_numpy = True
except:
    # No NumPy 
    pass
try:
    import pandas as pd
    _have_pandas = True
except:
    # No Pandas
    pass


def convert_to_pandas():
    if not _have_pandas:
        raise RuntimeError("Pandas is not available")
    pass

def convert_from_pandas():
    if not _have_pandas:
        raise RuntimeError("Pandas is not available")
    pass


def convert_from_numpy_to_python(convertable):
    if not _have_numpy:
        raise RuntimeError("Numpy is not available")

    if isinstance(convertable, list):
        return [convert_from_numpy_to_python(elem) for elem in convertable]
    elif isinstance(convertable, tuple):
        return (convertable[0], convert_from_numpy_to_python(convertable[1]))
    elif len(convertable) > 0 and isinstance(convertable[0], np.ndarray) and convertable[0].dtype.type == np.float64:
        return [array.array('d', elem) for elem in convertable]
    elif isinstance(convertable, np.ndarray):
        return array.array('d', convertable)
    elif isinstance(convertable, int) or isinstance(convertable, float):
        return convertable
    elif isinstance(convertable, str):
        raise TypeError
    else:
        raise ValueError('Could not convert input')


def convert_to_numpy_from_python(convertable):
    if not _have_numpy:
        raise RuntimeError("Numpy is not available")

    if isinstance(convertable, int) or isinstance(convertable, float) or isinstance(convertable, str):
        return convertable
    elif isinstance(convertable, list) or isinstance(convertable, tuple):
        return [convert_to_numpy_from_python(elem) for elem in convertable]
    elif isinstance(convertable, array.array):
        return np.frombuffer(convertable)
    elif isinstance(convertable[0], array.array):
        return [np.frombuffer(elem) for elem in convertable]
    else:
        raise ValueError('Could not convert input')




def convert_from_numpy_tuple_to_python(convertable):
    if not _have_numpy:
        raise RuntimeError("Numpy not avaiable")

    if isinstance(convertable, tuple):
        return [convertable[0], convert_from_numpy_to_python(convertable[1])]


def convert_from_pandas_to_python(convertable):
    if not _have_pandas:
        raise RuntimeError("Pandas is not available")

    try:
        if isinstance(convertable, pd.Series):
            return convert_from_numpy_to_python(convertable.values)
        elif isinstance(convertable, pd.DataFrame):
            return convert_from_numpy_to_python(convertable.values)
        else:
            raise ValueError('Could not convert input')

    except:
        raise ValueError('Could not convert input')


def convert_to_pandas_from_python(convertable):
    if not _have_pandas:
        raise RuntimeError("Pandas is not available")

    try:
        if isinstance(convertable[0], float):
            return pd.Series(convertable)
        elif isinstance(convertable[0], array.array):
            return pd.DataFrame(convert_to_numpy_from_python(convertable))
    except:
        raise ValueError('Could not convert input')