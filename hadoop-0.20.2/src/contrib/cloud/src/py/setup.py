# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from distutils.core import setup

version = __import__('hadoop.cloud').cloud.VERSION

setup(name='hadoop-cloud',
      version=version,
      description='Scripts for running Hadoop on cloud providers',
      license = 'Apache License (2.0)',
      url = 'http://hadoop.apache.org/common/',
      packages=['hadoop', 'hadoop.cloud','hadoop.cloud.providers'],
      package_data={'hadoop.cloud': ['data/*.sh']},
      scripts=['hadoop-ec2'],
      author = 'Apache Hadoop Contributors',
      author_email = 'common-dev@hadoop.apache.org',
)
