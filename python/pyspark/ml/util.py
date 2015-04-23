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

from functools import wraps
import uuid


def keyword_only(func):
    """
    A decorator that forces keyword arguments in the wrapped method
    and saves actual input keyword arguments in `_input_kwargs`.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if len(args) > 1:
            raise TypeError("Method %s forces keyword arguments." % func.__name__)
        wrapper._input_kwargs = kwargs
        return func(*args, **kwargs)
    return wrapper


class Identifiable(object):
    """
    Object with a unique ID.
    """

    def __init__(self):
        #: A unique id for the object. The default implementation
        #: concatenates the class name, "_", and 8 random hex chars.
        self.uid = type(self).__name__ + "_" + uuid.uuid4().hex[:8]

    def __repr__(self):
        return self.uid
