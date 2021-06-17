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

# $example on$
from pyspark.ml.feature import Interaction, VectorAssembler
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("InteractionExample")\
        .getOrCreate()

    # $example on$
    df = spark.createDataFrame(
        [(1, 1, 2, 3, 8, 4, 5),
         (2, 4, 3, 8, 7, 9, 8),
         (3, 6, 1, 9, 2, 3, 6),
         (4, 10, 8, 6, 9, 4, 5),
         (5, 9, 2, 7, 10, 7, 3),
         (6, 1, 1, 4, 2, 8, 4)],
        ["id1", "id2", "id3", "id4", "id5", "id6", "id7"])

    assembler1 = VectorAssembler(inputCols=["id2", "id3", "id4"], outputCol="vec1")

    assembled1 = assembler1.transform(df)

    assembler2 = VectorAssembler(inputCols=["id5", "id6", "id7"], outputCol="vec2")

    assembled2 = assembler2.transform(assembled1).select("id1", "vec1", "vec2")

    interaction = Interaction(inputCols=["id1", "vec1", "vec2"], outputCol="interactedCol")

    interacted = interaction.transform(assembled2)

    interacted.show(truncate=False)
    # $example off$

    spark.stop()
