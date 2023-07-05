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
import os
import subprocess
import shutil
from typing import List


def write_to_location(location: str, content: str) -> None:
    os.makedirs(os.path.dirname(location), exist_ok=True)
    with open(location, "a") as f:
        f.write(content)


class SSHEnvManager:
    """Is responsible for writing to the known_hosts file, setting up authorized users"""

    HOME = os.path.expanduser("~")
    KNOWN_HOSTS = f"{HOME}/.ssh/known_hosts"
    KNOWN_HOSTS_TEMP = f"/{HOME}/.ssh/known_hosts_temp"

    def __init__(self):
        # TODO: DECIDE IF MODIFYING THE KNOWN_HOSTS FILE IS ALLOWED OR IF THERE IS A BETTER WAY TO DO IT
        # self.known_hosts_exists = os.path.exists(SSHEnvManager.KNOWN_HOSTS)
        # if self.known_hosts_exists:
        #     shutil.copyfile(SSHEnvManager.KNOWN_HOSTS, SSHEnvManager.KNOWN_HOSTS_TEMP)
        pass

    def create_ssh_key(self, ssh_key_path: str):
        if os.path.exists(ssh_key_path):
            print(f"{ssh_key_path} already exists")
        else:
            print(f"Creating the ssh key to {ssh_key_path}")
            # the empty string at the end of this command is used to provide an empty passphrase
            cmd_status = subprocess.run(
                ["ssh-keygen", "-t", "rsa", "-f", ssh_key_path, "-q", "-N", ""], capture_output=True
            )
            if cmd_status.returncode != 0:
                raise RuntimeError(
                    f"Was unabled to create ssh-key to {ssh_key_path}\n. Output: {cmd_status.stdout.decode('utf-8')}"
                )

    def get_ssh_key(self, ssh_pub_key: str):
        with open(ssh_pub_key) as f:
            ssh_key = f.read()
        return ssh_key
