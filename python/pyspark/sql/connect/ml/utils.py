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

from pyspark.sql.utils import is_remote
import os


def _get_remote_ml_class(cls):
    remote_module = "pyspark.sql.connect.ml." + cls.__module__[len("pyspark.ml."):]
    cls_name = cls.__name__
    m = __import__(remote_module, fromlist=[cls_name])
    remote_cls = getattr(m, cls_name)
    return remote_cls


def try_remote_ml_class(x):

    @classmethod
    def patched__new__(cls, *args, **kwargs):
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            remote_cls = _get_remote_ml_class(cls)
            return remote_cls(*args[1:], **kwargs)

        obj = object.__new__(cls)
        obj.__init__(*args[1:], **kwargs)
        return obj

    x.__new__ = patched__new__
    return x


def try_remote_ml_classmethod(fn):

    def patched_fn(cls, *args, **kwargs):
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            remote_cls = _get_remote_ml_class(cls)
            original_fn_name = fn.__name__
            return getattr(remote_cls, original_fn_name)(*args, **kwargs)
        return fn(cls, *args, **kwargs)

    return patched_fn
