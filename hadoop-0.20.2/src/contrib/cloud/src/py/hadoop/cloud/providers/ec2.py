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

from boto.ec2.connection import EC2Connection
from boto.exception import EC2ResponseError
import logging
from hadoop.cloud.cluster import Cluster
from hadoop.cloud.cluster import Instance
from hadoop.cloud.cluster import RoleSyntaxException
from hadoop.cloud.cluster import TimeoutException
from hadoop.cloud.storage import JsonVolumeManager
from hadoop.cloud.storage import JsonVolumeSpecManager
from hadoop.cloud.storage import MountableVolume
from hadoop.cloud.storage import Storage
from hadoop.cloud.util import xstr
import os
import re
import subprocess
import sys
import time

logger = logging.getLogger(__name__)

def _run_command_on_instance(instance, ssh_options, command):
  print "Running ssh %s root@%s '%s'" % \
    (ssh_options, instance.public_dns_name, command)
  retcode = subprocess.call("ssh %s root@%s '%s'" %
                           (ssh_options, instance.public_dns_name, command),
                           shell=True)
  print "Command running on %s returned with value %s" % \
    (instance.public_dns_name, retcode)

def _wait_for_volume(ec2_connection, volume_id):
  """
  Waits until a volume becomes available.
  """
  while True:
    volumes = ec2_connection.get_all_volumes([volume_id,])
    if volumes[0].status == 'available':
      break
    sys.stdout.write(".")
    sys.stdout.flush()
    time.sleep(1)

class Ec2Cluster(Cluster):
  """
  A cluster of EC2 instances. A cluster has a unique name.

  Instances running in the cluster run in a security group with the cluster's
  name, and also a name indicating the instance's role, e.g. <cluster-name>-foo
  to show a "foo" instance.
  """

  @staticmethod
  def get_clusters_with_role(role, state="running"):
    all_instances = EC2Connection().get_all_instances()
    clusters = []
    for res in all_instances:
      instance = res.instances[0]
      for group in res.groups:
        if group.id.endswith("-" + role) and instance.state == state:
          clusters.append(re.sub("-%s$" % re.escape(role), "", group.id))
    return clusters

  def __init__(self, name, config_dir):
    super(Ec2Cluster, self).__init__(name, config_dir)
    self.ec2Connection = EC2Connection()

  def get_provider_code(self):
    return "ec2"

  def _get_cluster_group_name(self):
    return self.name

  def _check_role_name(self, role):
    if not re.match("^[a-zA-Z0-9_+]+$", role):
      raise RoleSyntaxException("Invalid role name '%s'" % role)

  def _group_name_for_role(self, role):
    """
    Return the security group name for an instance in a given role.
    """
    self._check_role_name(role)
    return "%s-%s" % (self.name, role)

  def _get_group_names(self, roles):
    group_names = [self._get_cluster_group_name()]
    for role in roles:
      group_names.append(self._group_name_for_role(role))
    return group_names

  def _get_all_group_names(self):
    security_groups = self.ec2Connection.get_all_security_groups()
    security_group_names = \
      [security_group.name for security_group in security_groups]
    return security_group_names

  def _get_all_group_names_for_cluster(self):
    all_group_names = self._get_all_group_names()
    r = []
    if self.name not in all_group_names:
      return r
    for group in all_group_names:
      if re.match("^%s(-[a-zA-Z0-9_+]+)?$" % self.name, group):
        r.append(group)
    return r

  def _create_groups(self, role):
    """
    Create the security groups for a given role, including a group for the
    cluster if it doesn't exist.
    """
    self._check_role_name(role)
    security_group_names = self._get_all_group_names()

    cluster_group_name = self._get_cluster_group_name()
    if not cluster_group_name in security_group_names:
      self.ec2Connection.create_security_group(cluster_group_name,
                                               "Cluster (%s)" % (self.name))
      self.ec2Connection.authorize_security_group(cluster_group_name,
                                                  cluster_group_name)
      # Allow SSH from anywhere
      self.ec2Connection.authorize_security_group(cluster_group_name,
                                                  ip_protocol="tcp",
                                                  from_port=22, to_port=22,
                                                  cidr_ip="0.0.0.0/0")

    role_group_name = self._group_name_for_role(role)
    if not role_group_name in security_group_names:
      self.ec2Connection.create_security_group(role_group_name,
        "Role %s (%s)" % (role, self.name))

  def authorize_role(self, role, from_port, to_port, cidr_ip):
    """
    Authorize access to machines in a given role from a given network.
    """
    self._check_role_name(role)
    role_group_name = self._group_name_for_role(role)
    # Revoke first to avoid InvalidPermission.Duplicate error
    self.ec2Connection.revoke_security_group(role_group_name,
                                             ip_protocol="tcp",
                                             from_port=from_port,
                                             to_port=to_port, cidr_ip=cidr_ip)
    self.ec2Connection.authorize_security_group(role_group_name,
                                                ip_protocol="tcp",
                                                from_port=from_port,
                                                to_port=to_port,
                                                cidr_ip=cidr_ip)

  def _get_instances(self, group_name, state_filter=None):
    """
    Get all the instances in a group, filtered by state.

    @param group_name: the name of the group
    @param state_filter: the state that the instance should be in
      (e.g. "running"), or None for all states
    """
    all_instances = self.ec2Connection.get_all_instances()
    instances = []
    for res in all_instances:
      for group in res.groups:
        if group.id == group_name:
          for instance in res.instances:
            if state_filter == None or instance.state == state_filter:
              instances.append(instance)
    return instances

  def get_instances_in_role(self, role, state_filter=None):
    """
    Get all the instances in a role, filtered by state.

    @param role: the name of the role
    @param state_filter: the state that the instance should be in
      (e.g. "running"), or None for all states
    """
    self._check_role_name(role)
    instances = []
    for instance in self._get_instances(self._group_name_for_role(role),
                                        state_filter):
      instances.append(Instance(instance.id, instance.dns_name,
                                instance.private_dns_name))
    return instances

  def _print_instance(self, role, instance):
    print "\t".join((role, instance.id,
      instance.image_id,
      instance.dns_name, instance.private_dns_name,
      instance.state, xstr(instance.key_name), instance.instance_type,
      str(instance.launch_time), instance.placement))

  def print_status(self, roles=None, state_filter="running"):
    """
    Print the status of instances in the given roles, filtered by state.
    """
    if not roles:
      for instance in self._get_instances(self._get_cluster_group_name(),
                                          state_filter):
        self._print_instance("", instance)
    else:
      for role in roles:
        for instance in self._get_instances(self._group_name_for_role(role),
                                            state_filter):
          self._print_instance(role, instance)

  def launch_instances(self, roles, number, image_id, size_id,
                       instance_user_data, **kwargs):
    for role in roles:
      self._check_role_name(role)  
      self._create_groups(role)
      
    user_data = instance_user_data.read_as_gzip_stream()
    security_groups = self._get_group_names(roles) + kwargs.get('security_groups', [])

    reservation = self.ec2Connection.run_instances(image_id, min_count=number,
      max_count=number, key_name=kwargs.get('key_name', None),
      security_groups=security_groups, user_data=user_data,
      instance_type=size_id, placement=kwargs.get('placement', None))
    return [instance.id for instance in reservation.instances]

  def wait_for_instances(self, instance_ids, timeout=600):
    start_time = time.time()
    while True:
      if (time.time() - start_time >= timeout):
        raise TimeoutException()
      try:
        if self._all_started(self.ec2Connection.get_all_instances(instance_ids)):
          break
      # don't timeout for race condition where instance is not yet registered
      except EC2ResponseError:
        pass
      sys.stdout.write(".")
      sys.stdout.flush()
      time.sleep(1)

  def _all_started(self, reservations):
    for res in reservations:
      for instance in res.instances:
        if instance.state != "running":
          return False
    return True

  def terminate(self):
    instances = self._get_instances(self._get_cluster_group_name(), "running")
    if instances:
      self.ec2Connection.terminate_instances([i.id for i in instances])

  def delete(self):
    """
    Delete the security groups for each role in the cluster, and the group for
    the cluster.
    """
    group_names = self._get_all_group_names_for_cluster()
    for group in group_names:
      self.ec2Connection.delete_security_group(group)

  def get_storage(self):
    """
    Return the external storage for the cluster.
    """
    return Ec2Storage(self)


class Ec2Storage(Storage):
  """
  Storage volumes for an EC2 cluster. The storage is associated with a named
  cluster. Metadata for the storage volumes is kept in a JSON file on the client
  machine (in a file called "ec2-storage-<cluster-name>.json" in the
  configuration directory).
  """

  @staticmethod
  def create_formatted_snapshot(cluster, size, availability_zone, image_id,
                                key_name, ssh_options):
    """
    Creates a formatted snapshot of a given size. This saves having to format
    volumes when they are first attached.
    """
    conn = cluster.ec2Connection
    print "Starting instance"
    reservation = conn.run_instances(image_id, key_name=key_name,
                                     placement=availability_zone)
    instance = reservation.instances[0]
    try:
      cluster.wait_for_instances([instance.id,])
      print "Started instance %s" % instance.id
    except TimeoutException:
      print "Timeout"
      return
    print
    print "Waiting 60 seconds before attaching storage"
    time.sleep(60)
    # Re-populate instance object since it has more details filled in
    instance.update()

    print "Creating volume of size %s in %s" % (size, availability_zone)
    volume = conn.create_volume(size, availability_zone)
    print "Created volume %s" % volume
    print "Attaching volume to %s" % instance.id
    volume.attach(instance.id, '/dev/sdj')

    _run_command_on_instance(instance, ssh_options, """
      while true ; do
        echo 'Waiting for /dev/sdj...';
        if [ -e /dev/sdj ]; then break; fi;
        sleep 1;
      done;
      mkfs.ext3 -F -m 0.5 /dev/sdj
    """)

    print "Detaching volume"
    conn.detach_volume(volume.id, instance.id)
    print "Creating snapshot"
    snapshot = volume.create_snapshot()
    print "Created snapshot %s" % snapshot.id
    _wait_for_volume(conn, volume.id)
    print
    print "Deleting volume"
    volume.delete()
    print "Deleted volume"
    print "Stopping instance"
    terminated = conn.terminate_instances([instance.id,])
    print "Stopped instance %s" % terminated

  def __init__(self, cluster):
    super(Ec2Storage, self).__init__(cluster)
    self.config_dir = cluster.config_dir

  def _get_storage_filename(self):
    return os.path.join(self.config_dir,
                        "ec2-storage-%s.json" % (self.cluster.name))

  def create(self, role, number_of_instances, availability_zone, spec_filename):
    spec_file = open(spec_filename, 'r')
    volume_spec_manager = JsonVolumeSpecManager(spec_file)
    volume_manager = JsonVolumeManager(self._get_storage_filename())
    for dummy in range(number_of_instances):
      mountable_volumes = []
      volume_specs = volume_spec_manager.volume_specs_for_role(role)
      for spec in volume_specs:
        logger.info("Creating volume of size %s in %s from snapshot %s" % \
                    (spec.size, availability_zone, spec.snapshot_id))
        volume = self.cluster.ec2Connection.create_volume(spec.size,
                                                          availability_zone,
                                                          spec.snapshot_id)
        mountable_volumes.append(MountableVolume(volume.id, spec.mount_point,
                                                 spec.device))
      volume_manager.add_instance_storage_for_role(role, mountable_volumes)

  def _get_mountable_volumes(self, role):
    storage_filename = self._get_storage_filename()
    volume_manager = JsonVolumeManager(storage_filename)
    return volume_manager.get_instance_storage_for_role(role)

  def get_mappings_string_for_role(self, role):
    mappings = {}
    mountable_volumes_list = self._get_mountable_volumes(role)
    for mountable_volumes in mountable_volumes_list:
      for mountable_volume in mountable_volumes:
        mappings[mountable_volume.mount_point] = mountable_volume.device
    return ";".join(["%s,%s" % (mount_point, device) for (mount_point, device)
                     in mappings.items()])

  def _has_storage(self, role):
    return self._get_mountable_volumes(role)

  def has_any_storage(self, roles):
    for role in roles:
      if self._has_storage(role):
        return True
    return False

  def get_roles(self):
    storage_filename = self._get_storage_filename()
    volume_manager = JsonVolumeManager(storage_filename)
    return volume_manager.get_roles()
  
  def _get_ec2_volumes_dict(self, mountable_volumes):
    volume_ids = [mv.volume_id for mv in sum(mountable_volumes, [])]
    volumes = self.cluster.ec2Connection.get_all_volumes(volume_ids)
    volumes_dict = {}
    for volume in volumes:
      volumes_dict[volume.id] = volume
    return volumes_dict

  def _print_volume(self, role, volume):
    print "\t".join((role, volume.id, str(volume.size),
                     volume.snapshot_id, volume.availabilityZone,
                     volume.status, str(volume.create_time),
                     str(volume.attach_time)))

  def print_status(self, roles=None):
    if roles == None:
      storage_filename = self._get_storage_filename()
      volume_manager = JsonVolumeManager(storage_filename)
      roles = volume_manager.get_roles()
    for role in roles:
      mountable_volumes_list = self._get_mountable_volumes(role)
      ec2_volumes = self._get_ec2_volumes_dict(mountable_volumes_list)
      for mountable_volumes in mountable_volumes_list:
        for mountable_volume in mountable_volumes:
          self._print_volume(role, ec2_volumes[mountable_volume.volume_id])

  def _replace(self, string, replacements):
    for (match, replacement) in replacements.iteritems():
      string = string.replace(match, replacement)
    return string

  def attach(self, role, instances):
    mountable_volumes_list = self._get_mountable_volumes(role)
    if not mountable_volumes_list:
      return
    ec2_volumes = self._get_ec2_volumes_dict(mountable_volumes_list)

    available_mountable_volumes_list = []

    available_instances_dict = {}
    for instance in instances:
      available_instances_dict[instance.id] = instance

    # Iterate over mountable_volumes and retain those that are not attached
    # Also maintain a list of instances that have no attached storage
    # Note that we do not fill in "holes" (instances that only have some of
    # their storage attached)
    for mountable_volumes in mountable_volumes_list:
      available = True
      for mountable_volume in mountable_volumes:
        if ec2_volumes[mountable_volume.volume_id].status != 'available':
          available = False
          attach_data = ec2_volumes[mountable_volume.volume_id].attach_data
          instance_id = attach_data.instance_id
          if available_instances_dict.has_key(instance_id):
            del available_instances_dict[instance_id]
      if available:
        available_mountable_volumes_list.append(mountable_volumes)

    if len(available_instances_dict) != len(available_mountable_volumes_list):
      logger.warning("Number of available instances (%s) and volumes (%s) \
        do not match." \
        % (len(available_instances_dict),
           len(available_mountable_volumes_list)))

    for (instance, mountable_volumes) in zip(available_instances_dict.values(),
                                             available_mountable_volumes_list):
      print "Attaching storage to %s" % instance.id
      for mountable_volume in mountable_volumes:
        volume = ec2_volumes[mountable_volume.volume_id]
        print "Attaching %s to %s" % (volume.id, instance.id)
        volume.attach(instance.id, mountable_volume.device)

  def delete(self, roles=[]):
    storage_filename = self._get_storage_filename()
    volume_manager = JsonVolumeManager(storage_filename)
    for role in roles:
      mountable_volumes_list = volume_manager.get_instance_storage_for_role(role)
      ec2_volumes = self._get_ec2_volumes_dict(mountable_volumes_list)
      all_available = True
      for volume in ec2_volumes.itervalues():
        if volume.status != 'available':
          all_available = False
          logger.warning("Volume %s is not available.", volume)
      if not all_available:
        logger.warning("Some volumes are still in use for role %s.\
          Aborting delete.", role)
        return
      for volume in ec2_volumes.itervalues():
        volume.delete()
      volume_manager.remove_instance_storage_for_role(role)
