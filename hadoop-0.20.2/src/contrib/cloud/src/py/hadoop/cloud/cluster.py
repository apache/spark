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

"""
Classes for controlling a cluster of cloud instances.
"""

from __future__ import with_statement

import gzip
import StringIO
import urllib

from hadoop.cloud.storage import Storage

CLUSTER_PROVIDER_MAP = {
  "dummy": ('hadoop.cloud.providers.dummy', 'DummyCluster'),
  "ec2": ('hadoop.cloud.providers.ec2', 'Ec2Cluster'),
}

def get_cluster(provider):
  """
  Retrieve the Cluster class for a provider.
  """
  mod_name, driver_name = CLUSTER_PROVIDER_MAP[provider]
  _mod = __import__(mod_name, globals(), locals(), [driver_name])
  return getattr(_mod, driver_name)

class Cluster(object):
  """
  A cluster of server instances. A cluster has a unique name.
  One may launch instances which run in a certain role.
  """

  def __init__(self, name, config_dir):
    self.name = name
    self.config_dir = config_dir

  def get_provider_code(self):
    """
    The code that uniquely identifies the cloud provider.
    """
    raise Exception("Unimplemented")

  def authorize_role(self, role, from_port, to_port, cidr_ip):
    """
    Authorize access to machines in a given role from a given network.
    """
    pass

  def get_instances_in_role(self, role, state_filter=None):
    """
    Get all the instances in a role, filtered by state.

    @param role: the name of the role
    @param state_filter: the state that the instance should be in
    (e.g. "running"), or None for all states
    """
    raise Exception("Unimplemented")

  def print_status(self, roles=None, state_filter="running"):
    """
    Print the status of instances in the given roles, filtered by state.
    """
    pass

  def check_running(self, role, number):
    """
    Check that a certain number of instances in a role are running.
    """
    instances = self.get_instances_in_role(role, "running")
    if len(instances) != number:
      print "Expected %s instances in role %s, but was %s %s" % \
        (number, role, len(instances), instances)
      return False
    else:
      return instances

  def launch_instances(self, roles, number, image_id, size_id,
                       instance_user_data, **kwargs):
    """
    Launch instances (having the given roles) in the cluster.
    Returns a list of IDs for the instances started.
    """
    pass

  def wait_for_instances(self, instance_ids, timeout=600):
    """
    Wait for instances to start.
    Raise TimeoutException if the timeout is exceeded.
    """
    pass

  def terminate(self):
    """
    Terminate all instances in the cluster.
    """
    pass

  def delete(self):
    """
    Delete the cluster permanently. This operation is only permitted if no
    instances are running.
    """
    pass

  def get_storage(self):
    """
    Return the external storage for the cluster.
    """
    return Storage(self)

class InstanceUserData(object):
  """
  The data passed to an instance on start up.
  """

  def __init__(self, filename, replacements={}):
    self.filename = filename
    self.replacements = replacements

  def _read_file(self, filename):
    """
    Read the user data.
    """
    return urllib.urlopen(filename).read()

  def read(self):
    """
    Read the user data, making replacements.
    """
    contents = self._read_file(self.filename)
    for (match, replacement) in self.replacements.iteritems():
      if replacement == None:
        replacement = ''
      contents = contents.replace(match, replacement)
    return contents

  def read_as_gzip_stream(self):
    """
    Read and compress the data.
    """
    output = StringIO.StringIO()
    compressed = gzip.GzipFile(mode='wb', fileobj=output)
    compressed.write(self.read())
    compressed.close()
    return output.getvalue()

class Instance(object):
  """
  A server instance.
  """
  def __init__(self, id, public_ip, private_ip):
    self.id = id
    self.public_ip = public_ip
    self.private_ip = private_ip

class RoleSyntaxException(Exception):
  """
  Raised when a role name is invalid. Role names may consist of a sequence
  of alphanumeric characters and underscores. Dashes are not permitted in role
  names.
  """
  def __init__(self, message):
    super(RoleSyntaxException, self).__init__()
    self.message = message
  def __str__(self):
    return repr(self.message)

class TimeoutException(Exception):
  """
  Raised when a timeout is exceeded.
  """
  pass
