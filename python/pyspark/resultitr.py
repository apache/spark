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

__all__ = ["ResultItr"]

import collections

class ResultItr(collections.Iterator):
    """
    A special result iterator. This is used because the standard iterator can not be pickled
    """
    def __init__(self, data):
        self.data = data
        self.index = 0
        self.maxindex = len(data)
    def next(self):
        if index == maxindex:
            raise StopIteration
        v = self.data[0]
        self.data = data[1:]
        return v
    def __iter__(self):
        return iter(self.data)
    def __len__(self):
        return len(self.data)
