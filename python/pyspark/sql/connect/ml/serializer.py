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

import pyspark.sql.connect.proto.ml_pb2 as ml_pb2


def deserialize(ml_command_result: ml_pb2.MlCommandResponse, client):
    if ml_command_result.HasField("literal"):
        literal = ml_command_result.literal
        if literal.HasField("integer"):
            return literal.integer
        if literal.HasField("double"):
            return literal.double
        raise ValueError()

    if ml_command_result.HasField("model_info"):
        model_info = ml_command_result.model_info
        return model_info.model_ref_id, model_info.model_uid

    raise ValueError()

