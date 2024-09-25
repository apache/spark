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

from typing import TYPE_CHECKING, Any

from pyspark.sql.plot import PySparkPlotAccessor

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from plotly.graph_objs import Figure


def plot_pyspark(data: "DataFrame", kind: str, **kwargs: Any) -> "Figure":
    import plotly

    return plotly.plot(PySparkPlotAccessor.plot_data_map[kind](data), kind, **kwargs)
