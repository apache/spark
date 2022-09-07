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

from pyspark.pandas.exceptions import PandasNotImplementedError


def unsupported_function(class_name, method_name, deprecated=False, reason=""):
    def unsupported_function(*args, **kwargs):
        raise PandasNotImplementedError(
            class_name=class_name, method_name=method_name, reason=reason
        )

    def deprecated_function(*args, **kwargs):
        raise PandasNotImplementedError(
            class_name=class_name, method_name=method_name, deprecated=deprecated, reason=reason
        )

    return deprecated_function if deprecated else unsupported_function


def unsupported_property(class_name, property_name, deprecated=False, reason=""):
    @property
    def unsupported_property(self):
        raise PandasNotImplementedError(
            class_name=class_name, property_name=property_name, reason=reason
        )

    @property
    def deprecated_property(self):
        raise PandasNotImplementedError(
            class_name=class_name, property_name=property_name, deprecated=deprecated, reason=reason
        )

    return deprecated_property if deprecated else unsupported_property
