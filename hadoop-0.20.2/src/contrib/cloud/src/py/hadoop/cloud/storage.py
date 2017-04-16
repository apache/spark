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
Classes for controlling external cluster storage.
"""

import logging
import simplejson as json

logger = logging.getLogger(__name__)

class VolumeSpec(object):
  """
  The specification for a storage volume, encapsulating all the information
  needed to create a volume and ultimately mount it on an instance.
  """
  def __init__(self, size, mount_point, device, snapshot_id):
    self.size = size
    self.mount_point = mount_point
    self.device = device
    self.snapshot_id = snapshot_id


class JsonVolumeSpecManager(object):
  """
  A container for VolumeSpecs. This object can read VolumeSpecs specified in
  JSON.
  """
  def __init__(self, spec_file):
    self.spec = json.load(spec_file)

  def volume_specs_for_role(self, role):
    return [VolumeSpec(d["size_gb"], d["mount_point"], d["device"],
                       d["snapshot_id"]) for d in self.spec[role]]

  def get_mappings_string_for_role(self, role):
    """
    Returns a short string of the form
    "mount_point1,device1;mount_point2,device2;..."
    which is useful for passing as an environment variable.
    """
    return ";".join(["%s,%s" % (d["mount_point"], d["device"])
                     for d in self.spec[role]])


class MountableVolume(object):
  """
  A storage volume that has been created. It may or may not have been attached
  or mounted to an instance.
  """
  def __init__(self, volume_id, mount_point, device):
    self.volume_id = volume_id
    self.mount_point = mount_point
    self.device = device


class JsonVolumeManager(object):

  def __init__(self, filename):
    self.filename = filename

  def _load(self):
    try:
      return json.load(open(self.filename, "r"))
    except IOError:
      logger.debug("File %s does not exist.", self.filename)
      return {}

  def _store(self, obj):
    return json.dump(obj, open(self.filename, "w"), sort_keys=True, indent=2)
  
  def get_roles(self):
    json_dict = self._load()
    return json_dict.keys()

  def add_instance_storage_for_role(self, role, mountable_volumes):
    json_dict = self._load()
    mv_dicts = [mv.__dict__ for mv in mountable_volumes]
    json_dict.setdefault(role, []).append(mv_dicts)
    self._store(json_dict)

  def remove_instance_storage_for_role(self, role):
    json_dict = self._load()
    del json_dict[role]
    self._store(json_dict)

  def get_instance_storage_for_role(self, role):
    """
    Returns a list of lists of MountableVolume objects. Each nested list is
    the storage for one instance.
    """
    try:
      json_dict = self._load()
      instance_storage = []
      for instance in json_dict[role]:
        vols = []
        for vol in instance:
          vols.append(MountableVolume(vol["volume_id"], vol["mount_point"],
                                      vol["device"]))
        instance_storage.append(vols)
      return instance_storage
    except KeyError:
      return []

class Storage(object):
  """
  Storage volumes for a cluster. The storage is associated with a named
  cluster. Many clusters just have local storage, in which case this is
  not used.
  """

  def __init__(self, cluster):
    self.cluster = cluster

  def create(self, role, number_of_instances, availability_zone, spec_filename):
    """
    Create new storage volumes for instances with the given role, according to
    the mapping defined in the spec file.
    """
    pass

  def get_mappings_string_for_role(self, role):
    """
    Returns a short string of the form
    "mount_point1,device1;mount_point2,device2;..."
    which is useful for passing as an environment variable.
    """
    raise Exception("Unimplemented")

  def has_any_storage(self, roles):
    """
    Return True if any of the given roles has associated storage
    """
    return False

  def get_roles(self):
    """
    Return a list of roles that have storage defined.
    """
    return []

  def print_status(self, roles=None):
    """
    Print the status of storage volumes for the given roles.
    """
    pass

  def attach(self, role, instances):
    """
    Attach volumes for a role to instances. Some volumes may already be
    attached, in which case they are ignored, and we take care not to attach
    multiple volumes to an instance.
    """
    pass

  def delete(self, roles=[]):
    """
    Permanently delete all the storage for the given roles.
    """
    pass
