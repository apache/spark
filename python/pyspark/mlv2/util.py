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

import pandas as pd
import cloudpickle


def aggregate_dataframe(
        dataframe,
        cols,
        local_agg_fn,
        merge_agg_status,
        agg_status_to_result
):
    """
    The function can be used to run arbitrary aggregation logic on a spark dataframe
    or a pandas dataframe.

    Parameters
    ----------
    dataframe :
        A spark dataframe or a pandas dataframe

    cols :
        The name of columns that are used in aggregation

    local_agg_fn :
        A user-defined function that converts a pandas dataframe to an object holding
        aggregation status. The aggregation status object must be pickle-able by
        `cloudpickle`.

    merge_agg_status :
        A user-defined function that merges 2 aggregation status objects into one and
        return the merged status. Either in-place modifying the first input status object
        and returning it or creating a new status object are acceptable.

    agg_status_to_result :
        A user-defined function that converts aggregation status object to final aggregation
        result.

    Returns
    -------
    Aggregation result.
    """

    if isinstance(dataframe, pd.DataFrame):
        dataframe = dataframe[list(cols)]
        agg_status = local_agg_fn(dataframe)
        return agg_status_to_result(agg_status)

    dataframe = dataframe.select(*cols)

    def compute_status(iterator):
        status = None

        for batch_pandas_df in iterator:
            new_batch_status = local_agg_fn(batch_pandas_df)
            if status is None:
                status = new_batch_status
            else:
                status = merge_agg_status(status, new_batch_status)

        return pd.DataFrame({'status': [cloudpickle.dumps(status)]})

    result_pdf = dataframe.mapInPandas(compute_status, schema='status binary').toPandas()

    merged_status = None
    for status in result_pdf.status:
        status = cloudpickle.loads(status)
        if merged_status is None:
            merged_status = status
        else:
            merged_status = merge_agg_status(merged_status, status)

    return agg_status_to_result(merged_status)
