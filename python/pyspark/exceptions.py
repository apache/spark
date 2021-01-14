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

class NonDriverReferenceError(Exception):
    """
    Thrown when trying to reference a driver's value or attribute in a non driver environment.
    """


class JavaGatewayError(Exception):
    """
        Indicates an error of the java gateway.
    """


class SparkContextInitializationError(RuntimeError):
    """
        Error in the process of initializing a spark context.

        Subclasses from RuntimeError for backward compatability reasons.
    """


class SparkContextConfigurationError(SparkContextInitializationError):
    """
    Thrown whenever there is a Spark's configuration related problem.

    """
