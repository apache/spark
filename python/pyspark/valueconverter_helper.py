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


def numpy_to_python(convertable):
    if not _have_numpy:
        raise RuntimeError('Numpy is not available')

    try:
        iter = iter(convertable)
        return [numpy_to_python(elem) for elem in iter]
    except TypeError:
        if isinstance(convertable, tuple):
            return convertable[0], numpy_to_python(convertable[1])
        elif isinstance(convertable, np.ndarray):
            return array.array('d', convertable)
        elif isinstance(convertable, int) or isinstance(convertable, float):
            return convertable
        elif isinstance(convertable, str):
            raise TypeError
        elif isinstance(convertable[0], np.ndarray) and convertable[0].dtype.type == np.float64:
            return [array.array('d', elem) for elem in convertable]
        else:
            raise ValueError('Could not convert input')
    except IndexError:
        raise ValueError('Could not convert input: Misshaped input')


def python_to_numpy(convertable):
    if not _have_numpy:
        raise RuntimeError('Numpy is not available')

    try:
        if isinstance(convertable, int) or isinstance(convertable, float) or isinstance(convertable, str):
            return convertable
        elif isinstance(convertable, list) or isinstance(convertable, tuple):
            return [python_to_numpy(elem) for elem in convertable]
        elif isinstance(convertable, array.array):
            return np.frombuffer(convertable)
        elif isinstance(convertable[0], array.array):
            return [np.frombuffer(elem) for elem in convertable]
        else:
            raise ValueError('Could not convert input')
    except IndexError:
        raise ValueError('Could not convert input: Misshaped input')


def pandas_to_python(convertable):
    if not _have_pandas:
        raise RuntimeError('Pandas is not available')

    try:
        if isinstance(convertable, pd.Series):
            return numpy_to_python(convertable.values)
        elif isinstance(convertable, pd.DataFrame):
            return numpy_to_python(convertable.values)
        else:
            raise ValueError('Could not convert input')
    except IndexError:
        raise ValueError('Could not convert input: Misshaped input')
    except:
        raise ValueError('Could not convert input')


def python_to_pandas(convertable):
    if not _have_pandas:
        raise RuntimeError('Pandas is not available')

    try:
        if isinstance(convertable[0], float):
            return pd.Series(convertable)
        elif isinstance(convertable[0], array.array):
            return pd.DataFrame(python_to_numpy(convertable))
    except IndexError:
        raise ValueError('Could not convert input: Misshaped input')
    except:
        raise ValueError('Could not convert input')

