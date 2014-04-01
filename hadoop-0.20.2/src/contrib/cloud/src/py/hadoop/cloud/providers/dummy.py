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

import logging

from hadoop.cloud.cluster import Cluster
from hadoop.cloud.cluster import Instance

logger = logging.getLogger(__name__)

class DummyCluster(Cluster):

  @staticmethod
  def get_clusters_with_role(role, state="running"):
    logger.info("get_clusters_with_role(%s, %s)", role, state)
    return ["dummy-cluster"]

  def __init__(self, name, config_dir):
    super(DummyCluster, self).__init__(name, config_dir)
    logger.info("__init__(%s, %s)", name, config_dir)

  def get_provider_code(self):
    return "dummy"

  def authorize_role(self, role, from_port, to_port, cidr_ip):
    logger.info("authorize_role(%s, %s, %s, %s)", role, from_port, to_port,
                cidr_ip)

  def get_instances_in_role(self, role, state_filter=None):
    logger.info("get_instances_in_role(%s, %s)", role, state_filter)
    return [Instance(1, '127.0.0.1', '127.0.0.1')]

  def print_status(self, roles, state_filter="running"):
    logger.info("print_status(%s, %s)", roles, state_filter)

  def launch_instances(self, role, number, image_id, size_id,
                       instance_user_data, **kwargs):
    logger.info("launch_instances(%s, %s, %s, %s, %s, %s)", role, number,
                image_id, size_id, instance_user_data, str(kwargs))
    return [1]

  def wait_for_instances(self, instance_ids, timeout=600):
    logger.info("wait_for_instances(%s, %s)", instance_ids, timeout)

  def terminate(self):
    logger.info("terminate")

  def delete(self):
    logger.info("delete")
