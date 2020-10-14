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


class ResourceInformation(object):

    """
    .. note:: Evolving

    Class to hold information about a type of Resource. A resource could be a GPU, FPGA, etc.
    The array of addresses are resource specific and its up to the user to interpret the address.

    One example is GPUs, where the addresses would be the indices of the GPUs

    :param name: the name of the resource
    :param addresses: an array of strings describing the addresses of the resource

    .. versionadded:: 3.0.0
    """

    def __init__(self, name, addresses):
        self._name = name
        self._addresses = addresses

    @property
    def name(self):
        return self._name

    @property
    def addresses(self):
        return self._addresses
