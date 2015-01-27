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

from pyspark.sql import inherit_doc
from pyspark.ml import JavaTransformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, HasNumFeatures


@inherit_doc
class Tokenizer(JavaTransformer, HasInputCol, HasOutputCol):

    def __init__(self):
        super(Tokenizer, self).__init__()

    @property
    def _java_class(self):
        return "org.apache.spark.ml.feature.Tokenizer"


@inherit_doc
class HashingTF(JavaTransformer, HasInputCol, HasOutputCol, HasNumFeatures):

    def __init__(self):
        super(HashingTF, self).__init__()

    @property
    def _java_class(self):
        return "org.apache.spark.ml.feature.HashingTF"
