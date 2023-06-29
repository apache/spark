import os
import subprocess
import shutil
from typing import (List)

from pyspark.ml.torch.deepspeed_distributer import _write_to_location

class SSHEnvManager:
    """Is responsible for writing to the known_hosts file, setting up authorized users, and more"""
    KNOWN_HOSTS = "/root/.ssh/known_hosts"
    KNOWN_HOSTS_TEMP = "/root/.ssh/known_hosts_temp"

    def __init__(self):
        self.known_hosts_exists = os.path.exists(SSHEnvManager.KNOWN_HOSTS)
        if self.known_hosts_exists:
            shutil.copyfile(SSHEnvManager.KNOWN_HOSTS, SSHEnvManager.KNOWN_HOSTS_TEMP)

    def create_ssh_key(self, ssh_key_path: str):
        if not os.path.exists(ssh_key_path):
            print(f"Creating the ssh key to {ssh_key_path}")
            cmd_status = subprocess.run(["ssh-keygen", "-t", "rsa", "-f", ssh_key_path, "-q", "-N", ""])
            if cmd_status.returncode:
                raise RuntimeError(f"Was unabled to create ssh-key to {ssh_key_path}")
        else:
            print(f"{ssh_key_path} already exists")

    def get_ssh_key(self, ssh_pub_key: str):
        with open(ssh_pub_key) as f:
            ssh_key = f.read()
        return ssh_key

    def ssh_keyscan(self, ip_list: List[str]):
        """Is used to allow ssh to not prompt us `Are you sure you want to connect`, thus removing user need to use the terminal"""
        # if there is a known_hosts file, we need to preserve old one as we modify it
        # otherwise, just write to it
        print("Trying to add the worker node public ssh keys to the ssh known_hosts file")
        for ip in ip_list:
            cmd_args = ["ssh-keyscan", ip]
            error_code = subprocess.run(cmd_args, capture_output=True)
            if error_code.returncode:
                raise RuntimeError(f"Something went wrong when running ssh_keyscan {ip}. Command tried to run: ", cmd_args)
            cmd_output = error_code.stdout.decode('utf-8') # get the output from the command so we can write to right location
            _write_to_location(SSHEnvManager.KNOWN_HOSTS, cmd_output)
        print("Successfully finished writing worker ssh public keys to known_hosts on driver")

    def cleanup_ssh_env(self):
        try:
            os.remove(SSHEnvManager.KNOWN_HOSTS)
        except OSError:
            raise OSError("Wow something went wrong when cleaning up known_hosts.")
        if self.known_hosts_exists:
            try:
                os.rename(SSHEnvManager.KNOWN_HOSTS_TEMP, SSHEnvManager.KNOWN_HOSTS)
            except OSError:
                raise OSError("Couldn't rename the original known_hosts file - I wonder why this went wrong")
