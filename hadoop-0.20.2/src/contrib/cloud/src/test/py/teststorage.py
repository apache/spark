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

import os
import unittest

import simplejson as json
from StringIO import StringIO

from hadoop.cloud.storage import MountableVolume
from hadoop.cloud.storage import JsonVolumeManager
from hadoop.cloud.storage import JsonVolumeSpecManager

spec = {
 "master": ({"size_gb":"8", "mount_point":"/", "device":"/dev/sdj",
             "snapshot_id": "snap_1"},
            ),
 "slave": ({"size_gb":"8", "mount_point":"/", "device":"/dev/sdj",
            "snapshot_id": "snap_2"},
           {"size_gb":"10", "mount_point":"/data1", "device":"/dev/sdk",
            "snapshot_id": "snap_3"},
           )
 }

class TestJsonVolumeSpecManager(unittest.TestCase):

  def test_volume_specs_for_role(self):

    input = StringIO(json.dumps(spec))

    volume_spec_manager = JsonVolumeSpecManager(input)

    master_specs = volume_spec_manager.volume_specs_for_role("master")
    self.assertEqual(1, len(master_specs))
    self.assertEqual("/", master_specs[0].mount_point)
    self.assertEqual("8", master_specs[0].size)
    self.assertEqual("/dev/sdj", master_specs[0].device)
    self.assertEqual("snap_1", master_specs[0].snapshot_id)

    slave_specs = volume_spec_manager.volume_specs_for_role("slave")
    self.assertEqual(2, len(slave_specs))
    self.assertEqual("snap_2", slave_specs[0].snapshot_id)
    self.assertEqual("snap_3", slave_specs[1].snapshot_id)

    self.assertRaises(KeyError, volume_spec_manager.volume_specs_for_role,
                      "no-such-role")

  def test_get_mappings_string_for_role(self):

    input = StringIO(json.dumps(spec))

    volume_spec_manager = JsonVolumeSpecManager(input)

    master_mappings = volume_spec_manager.get_mappings_string_for_role("master")
    self.assertEqual("/,/dev/sdj", master_mappings)

    slave_mappings = volume_spec_manager.get_mappings_string_for_role("slave")
    self.assertEqual("/,/dev/sdj;/data1,/dev/sdk", slave_mappings)

    self.assertRaises(KeyError,
                      volume_spec_manager.get_mappings_string_for_role,
                      "no-such-role")

class TestJsonVolumeManager(unittest.TestCase):

  def tearDown(self):
    try:
      os.remove("volumemanagertest.json")
    except OSError:
      pass
    
  def test_add_instance_storage_for_role(self):
    volume_manager = JsonVolumeManager("volumemanagertest.json")
    self.assertEqual(0,
      len(volume_manager.get_instance_storage_for_role("master")))
    self.assertEqual(0, len(volume_manager.get_roles()))

    volume_manager.add_instance_storage_for_role("master",
                                                 [MountableVolume("vol_1", "/",
                                                                  "/dev/sdj")])
    master_storage = volume_manager.get_instance_storage_for_role("master")
    self.assertEqual(1, len(master_storage))
    master_storage_instance0 = master_storage[0]
    self.assertEqual(1, len(master_storage_instance0))
    master_storage_instance0_vol0 = master_storage_instance0[0]
    self.assertEqual("vol_1", master_storage_instance0_vol0.volume_id)
    self.assertEqual("/", master_storage_instance0_vol0.mount_point)
    self.assertEqual("/dev/sdj", master_storage_instance0_vol0.device)

    volume_manager.add_instance_storage_for_role("slave",
                                                 [MountableVolume("vol_2", "/",
                                                                  "/dev/sdj")])
    self.assertEqual(1,
      len(volume_manager.get_instance_storage_for_role("master")))
    slave_storage = volume_manager.get_instance_storage_for_role("slave")
    self.assertEqual(1, len(slave_storage))
    slave_storage_instance0 = slave_storage[0]
    self.assertEqual(1, len(slave_storage_instance0))
    slave_storage_instance0_vol0 = slave_storage_instance0[0]
    self.assertEqual("vol_2", slave_storage_instance0_vol0.volume_id)
    self.assertEqual("/", slave_storage_instance0_vol0.mount_point)
    self.assertEqual("/dev/sdj", slave_storage_instance0_vol0.device)

    volume_manager.add_instance_storage_for_role("slave",
      [MountableVolume("vol_3", "/", "/dev/sdj"),
       MountableVolume("vol_4", "/data1", "/dev/sdk")])
    self.assertEqual(1,
      len(volume_manager.get_instance_storage_for_role("master")))
    slave_storage = volume_manager.get_instance_storage_for_role("slave")
    self.assertEqual(2, len(slave_storage))
    slave_storage_instance0 = slave_storage[0]
    slave_storage_instance1 = slave_storage[1]
    self.assertEqual(1, len(slave_storage_instance0))
    self.assertEqual(2, len(slave_storage_instance1))
    slave_storage_instance1_vol0 = slave_storage_instance1[0]
    slave_storage_instance1_vol1 = slave_storage_instance1[1]
    self.assertEqual("vol_3", slave_storage_instance1_vol0.volume_id)
    self.assertEqual("/", slave_storage_instance1_vol0.mount_point)
    self.assertEqual("/dev/sdj", slave_storage_instance1_vol0.device)
    self.assertEqual("vol_4", slave_storage_instance1_vol1.volume_id)
    self.assertEqual("/data1", slave_storage_instance1_vol1.mount_point)
    self.assertEqual("/dev/sdk", slave_storage_instance1_vol1.device)
    
    roles = volume_manager.get_roles()
    self.assertEqual(2, len(roles))
    self.assertTrue("slave" in roles)
    self.assertTrue("master" in roles)


if __name__ == '__main__':
  unittest.main()
