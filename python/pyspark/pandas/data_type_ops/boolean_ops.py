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

from pyspark.pandas.data_type_ops.base import DataTypeOps


class BooleanOps(DataTypeOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark type: BooleanType.
    """

    def __add__(self, left, right):
        raise TypeError("addition can not be applied to given types.")

    def __sub__(self, left, right):
        raise TypeError("subtraction can not be applied to given types.")

    def __mul__(self, left, right):
        raise TypeError("multiplication can not be applied to given types.")

    def __truediv__(self, left, right):
        raise TypeError("division can not be applied to given types.")

    def __floordiv__(self, left, right):
        raise TypeError("division can not be applied to given types.")

    def __mod__(self, left, right):
        raise TypeError("modulo can not be applied to given types.")

    def __pow__(self, left, right):
        raise TypeError("exponentiation can not be applied to given types.")

    def __radd__(self, left, right=None):
        raise TypeError("addition can not be applied to given types.")

    def __rsub__(self, left, right=None):
        raise TypeError("subtraction can not be applied to given types.")

    def __rmul__(self, left, right=None):
        raise TypeError("multiplication can not be applied to given types.")

    def __rtruediv__(self, left, right=None):
        raise TypeError("division can not be applied to given types.")

    def __rfloordiv__(self, left, right=None):
        raise TypeError("division can not be applied to given types.")

    def __rpow__(self, left, right=None):
        raise TypeError("exponentiation can not be applied to given types.")

    def __rmod__(self, left, right=None):
        raise TypeError("modulo can not be applied to given types.")
