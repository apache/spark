#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Module: tests.py

This module holds all unit tests defined for the deployment to EC2
functionality of Spark.  Run this module directly to execute the tests:

    $ python tests.py
"""
import unittest

from boto import ec2
import mock
import moto

import spark_ec2


class CommandTests(unittest.TestCase):
    """
    CommandTests defines unit tests for the commands that can be passed to the
    deployment script: launch, destroy, login, etc ...
    """

    @moto.mock_ec2
    def test_destroy(self):
        spark_ec2.AWS_EVENTUAL_CONSISTENCY = 1
        opts = mock.MagicMock(name='opts')
        opts.region = "us-east-1"
        conn = ec2.connect_to_region(opts.region)
        cluster_name = "cluster_name"
        try:
            spark_ec2.destroy_cluster(conn, opts, cluster_name)
        except:
            self.fail("destroy_cluster raised unexpected exception")

if __name__ == '__main__':
    unittest.main()
