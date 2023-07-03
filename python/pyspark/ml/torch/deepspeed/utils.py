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
        self.known_hosts_exists = os.path.exists(SSHEnvManager.KNOWN_HOSTS)
        if self.known_hosts_exists:
            shutil.copyfile(SSHEnvManager.KNOWN_HOSTS, SSHEnvManager.KNOWN_HOSTS_TEMP)

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

    def ssh_keyscan(self, ip_list: List[str]):
        """Runs the ssh-keyscan on each IP in the ip_list and then writes the public key of that IP to the known_hosts file in ssh"""
        # if there is a known_hosts file, we need to preserve old one as we modify it
        # otherwise, just write to it
        print("Trying to add the worker node public ssh keys to the ssh known_hosts file")
        for ip in ip_list:
            cmd_args = ["ssh-keyscan", ip]
            error_code = subprocess.run(cmd_args, capture_output=True)
            if error_code.returncode != 0:
                raise RuntimeError(
                    f"Something went wrong when running ssh_keyscan {ip}. Command tried to run: ",
                    cmd_args,
                )
            cmd_output = error_code.stdout.decode(
                "utf-8"
            )  # get the output from the command so we can write to right location
            write_to_location(SSHEnvManager.KNOWN_HOSTS, cmd_output)
        print("Successfully finished writing worker ssh public keys to known_hosts on driver")

    def cleanup_ssh_env(self):
        try:
            os.remove(SSHEnvManager.KNOWN_HOSTS)
        except OSError:
            raise RuntimeError(
                "Wow something went wrong when cleaning up known_hosts. I couldn't remove ",
                SSHEnvManager.KNOWN_HOSTS,
            )
        if self.known_hosts_exists:
            try:
                os.rename(SSHEnvManager.KNOWN_HOSTS_TEMP, SSHEnvManager.KNOWN_HOSTS)
            except OSError:
                raise RuntimeError(
                    "Couldn't rename the original known_hosts file - I wonder why this went wrong"
                )
