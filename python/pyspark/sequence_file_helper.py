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

# Convert Python array.array to np.array
def convert_to_numpy(input):
    if not _have_numpy:
        raise RuntimeError("NumPy is not available")

    return [(elem[0], np.frombuffer(elem[1])) for elem in input]


def convert_to_nested_numpy(input):
    if not _have_numpy:
        raise RuntimeError("NumPy is not available")

    return [(elem[0], np.array([np.array(nested_ar) for nested_ar in elem[1]])) for elem in input]


def convert_from_numpy(input):
    if not _have_numpy:
        raise RuntimeError("NumPy is not available")

    return [(elem[0], array.array('d', elem[1])) for elem in input]


def convert_from_nested_numpy(input):
    if not _have_numpy:
        raise RuntimeError("NumPy is not available")

    return [(elem[0], [nested_ar.tolist() for nested_ar in elem[1]]) for elem in input]
