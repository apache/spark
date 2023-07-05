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

import contextlib
from contextlib import contextmanager
import os
import unittest
import shutil

from pyspark.ml.torch.deepspeed.utils import SSHEnvManager, write_to_location

KNOWN_HOSTS_TEST_TEMP = "/root/.ssh/known_hosts_before_test"
KNOWN_HOSTS = "/root/.ssh/known_hosts"
PRIVATE_SSH_KEY = "private test key here"
PUBLIC_SSH_KEY = "public test key here"

# TODO: restore the tests that are commented out once a decision regarding the
# known_hosts file and whatnot


class SSHTestEnvSetup:
    def __init__(self):
        # self.save_known_hosts()
        pass

    def save_known_hosts(self):
        if os.path.exists(KNOWN_HOSTS):
            shutil.move(KNOWN_HOSTS, KNOWN_HOSTS_TEST_TEMP)

    def restore_known_hosts(self):
        if os.path.exists(KNOWN_HOSTS_TEST_TEMP):
            shutil.move(KNOWN_HOSTS_TEST_TEMP, KNOWN_HOSTS)

    @contextmanager
    def create_ssh_key_file(self, ssh_key_path: str):
        if not os.path.exists(ssh_key_path):
            with open(ssh_key_path, "w+") as f:
                f.write(PRIVATE_SSH_KEY)
            with open(f"{ssh_key_path}.pub", "w+") as f:
                f.write(PUBLIC_SSH_KEY)
            yield
            os.remove(ssh_key_path)
            os.remove(f"{ssh_key_path}.pub")

    @contextmanager
    def temp_remove_ssh_key_file(self, ssh_key_path: str):
        if os.path.exists(ssh_key_path):
            shutil.move(ssh_key_path, f"{ssh_key_path}.temp")
        if os.path.exists(f"{ssh_key_path}.pub"):
            shutil.move(f"{ssh_key_path}.pub", f"{ssh_key_path}.pub.temp")
        yield
        if os.path.exists(f"ssh_key_path.temp"):
            shutil.move(f"{ssh_key_path}.temp", ssh_key_path)
        if os.path.exists(f"{ssh_key_path}.pub.temp"):
            shutil.move(f"{ssh_key_path}.pub.temp", "{ssh_key_path}.pub")

    @contextmanager
    def create_known_hosts(self, msg=""):
        write_to_location("/root/.ssh/known_hosts", msg)
        yield
        os.remove("/root/.ssh/known_hosts")


class TestSSHEnvManager(unittest.TestCase):
    def create_ssh_key_test(self):
        ssh_test_env = SSHTestEnvSetup()
        ssh_key_test_path = "/root/.ssh/test_key"
        with self.subTest(msg="Check if it creates a key when there is no file with that name"):
            with ssh_test_env.temp_remove_ssh_key_file(ssh_key_test_path):
                ssh_env_manager = SSHEnvManager()
                ssh_env_manager.create_ssh_key(ssh_key_test_path)
                self.assertTrue(os.path.exists(ssh_key_test_path))
                self.assertTrue(os.path.exists(f"{ssh_key_test_path}.pub"))
                os.remove(ssh_key_test_path)
                os.remove(f"{ssh_key_test_path}.pub")

    def get_ssh_key_test(self):
        ssh_test_env = SSHTestEnvSetup()
        ssh_key_test_path = "/root/.ssh/test_key"
        with self.subTest(msg="Try and make sure it is able to get the key properly"):
            ssh_env_manager = SSHEnvManager()
            with ssh_test_env.create_ssh_key_file(ssh_key_test_path):
                ssh_public_key = ssh_env_manager.get_ssh_key(f"{ssh_key_test_path}.pub")
                self.assertEqual(ssh_public_key, PUBLIC_SSH_KEY)
                ssh_private_key = ssh_env_manager.get_ssh_key(ssh_key_test_path)
                self.assertEqual(ssh_private_key, PRIVATE_SSH_KEY)


if __name__ == "__main__":
    unittest.main()
