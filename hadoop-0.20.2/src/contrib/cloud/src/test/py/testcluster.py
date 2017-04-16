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

import unittest

from hadoop.cloud.cluster import RoleSyntaxException
from hadoop.cloud.providers.ec2 import Ec2Cluster

class TestCluster(unittest.TestCase):

  def test_group_name_for_role(self):
    cluster = Ec2Cluster("test-cluster", None)
    self.assertEqual("test-cluster-foo", cluster._group_name_for_role("foo"))

  def test_check_role_name_valid(self):
    cluster = Ec2Cluster("test-cluster", None)
    cluster._check_role_name(
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_+")

  def test_check_role_name_dash_is_invalid(self):
    cluster = Ec2Cluster("test-cluster", None)
    self.assertRaises(RoleSyntaxException, cluster._check_role_name, "a-b")

if __name__ == '__main__':
  unittest.main()
