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
from inspect import FullArgSpec
from typing import List, Optional, Type, cast as _cast  # noqa: F401

import numpy as np  # noqa: F401
import pandas  # noqa: F401
import pandas as pd  # noqa: F401
from numpy import *  # noqa: F401
from pandas import *  # type: ignore[no-redef] # noqa: F401
from inspect import getfullargspec  # noqa: F401


def resolve_string_type_hint(tpe: str) -> Optional[Type]:
    import pyspark.pandas as ps
    from pyspark.pandas import DataFrame, Series  # type: ignore[misc]

    locs = {
        "ps": ps,
        "pyspark.pandas": ps,
        "DataFrame": DataFrame,
        "Series": Series,
    }
    # This is a hack to resolve the forward reference string.
    exec("def func() -> %s: pass\narg_spec = getfullargspec(func)" % tpe, globals(), locs)
    return _cast(FullArgSpec, locs["arg_spec"]).annotations.get("return", None)
