import contextlib
from contextlib import contextmanager
import os
import unittest
import shutil
from pyspark.ml.torch import ssh_manager

from pyspark.ml.torch.ssh_manager import SSHEnvManager
from pyspark.ml.torch.ssh_manager import write_to_location

KNOWN_HOSTS_TEST_TEMP = "/root/.ssh/known_hosts_before_test"
KNOWN_HOSTS = "/root/.ssh/known_hosts"
PRIVATE_SSH_KEY = "private test key here"
PUBLIC_SSH_KEY = "public test key here"


class SSHTestEnvSetup():
    def __init__(self):
        self.save_known_hosts() 
    
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
            shutil.move(f"{ssh_key_path}.pub.temp","{ssh_key_path}.pub")
    
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
                self.assertEqual(ssh_public_key,PUBLIC_SSH_KEY)
                ssh_private_key = ssh_env_manager.get_ssh_key(ssh_key_test_path)
                self.assertEqual(ssh_private_key, PRIVATE_SSH_KEY)

    def saves_original_known_hosts_file_test(self):
        ssh_test_env = SSHTestEnvSetup()        
        with self.subTest(msg="Empty known hosts file"):
            with ssh_test_env.create_known_hosts():
                ssh_manager = SSHEnvManager()
                self.assertTrue(os.path.exists('/root/.ssh/known_host'))
                self.assertTrue(os.path.exists(SSHEnvManager.KNOWN_HOSTS_TEMP))
            os.remove(SSHEnvManager.KNOWN_HOSTS_TEMP)
       
        with self.subTest(msg="Non-empty known_hosts_file"):
            SSH_MSG = "this is a message"
            with ssh_test_env.create_known_hosts(msg=SSH_MSG):
                ssh_manager = SSHEnvManager()
                self.assertTrue(os.path.exists('/root/.ssh/known_host'))
                self.assertTrue(os.path.exists(SSHEnvManager.KNOWN_HOSTS_TEMP))
                with open(SSHEnvManager.KNOWN_HOSTS_TEMP) as f:
                    contents = f.read()
                    self.assertEqual(contents, SSH_MSG)
                with open(SSHEnvManager.KNOWN_HOSTS):
                    contents = f.read()
                    self.assertEqual(contents, SSH_MSG)
            os.remove(SSHEnvManager.KNOWN_HOSTS)
            os.remove(SSHEnvManager.KNOWN_HOSTS_TEMP)

    def cleanup_ssh_env_test(self):
        ssh_test_env = SSHTestEnvSetup()
        with self.subTest(msg="Checking to see if the SSHEnvManager cleans up an empty file"):
            with ssh_test_env.create_known_hosts():
                ssh_env_manager = SSHEnvManager()
                self.assertTrue(os.path.exists(SSHEnvManager.KNOWN_HOSTS_TEMP))
                ssh_env_manager.cleanup_ssh_env() 
                self.assertFalse(os.path.exists(SSHEnvManager.KNOWN_HOSTS_TEMP))
                self.assertTrue(os.path.exists(SSHEnvManager.KNOWN_HOSTS))
        with self.subTest(msg="Checking to see if the SSHEnvManager correctly cleans an existing file") :
            with ssh_test_env.create_known_hosts(msg=PUBLIC_SSH_KEY):
                ssh_env_manager = SSHEnvManager()
                self.assertTrue(os.path.exists(SSHEnvManager.KNOWN_HOSTS_TEMP))
                with open(SSHEnvManager.KNOWN_HOSTS_TEMP, "r") as f:
                    contents = f.read()
                    self.assertEqual(contents, PUBLIC_SSH_KEY)
                ssh_env_manager.cleanup_ssh_env() 
                self.assertFalse(os.path.exists(SSHEnvManager.KNOWN_HOSTS_TEMP))
                self.assertTrue(os.path.exists(SSHEnvManager.KNOWN_HOSTS))
                with open(SSHEnvManager.KNOWN_HOSTS_TEMP, 'r') as f:
                    contents = f.read()
                    self.assertEqual(contents, PUBLIC_SSH_KEY)


if __name__ == '__main__':
    unittest.main()
