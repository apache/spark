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

import os


__all__ = ['SparkFiles']


class SparkFiles(object):

    """
    Resolves paths to files added through
    L{SparkContext.addFile()<pyspark.context.SparkContext.addFile>}.

    SparkFiles contains only classmethods; users should not create SparkFiles
    instances.
    """

    _root_directory = None
    _is_running_on_worker = False
    _sc = None

    def __init__(self):
        raise NotImplementedError("Do not construct SparkFiles objects")

    @classmethod
    def get(cls, filename):
        """
        Get the absolute path of a file added through C{SparkContext.addFile()}.
        """
        path = os.path.join(SparkFiles.getRootDirectory(), filename)
        return os.path.abspath(path)

    @classmethod
    def getRootDirectory(cls):
        """
        Get the root directory that contains files added through
        C{SparkContext.addFile()}.
        """
        if cls._is_running_on_worker:
            return cls._root_directory
        else:
            # This will have to change if we support multiple SparkContexts:
            return cls._sc._jvm.org.apache.spark.SparkFiles.getRootDirectory()
