from smbclient import SambaClient
import os

from airflow.hooks.base_hook import BaseHook


class SambaHook(BaseHook):
    '''
    Allows for interaction with an samba server.
    '''

    def __init__(self, samba_conn_id):
        self.conn = self.get_connection(samba_conn_id)

    def get_conn(self):
        samba = SambaClient(
            server=self.conn.host,
            share=self.conn.schema,
            username=self.conn.login,
            ip=self.conn.host,
            password=self.conn.password)
        return samba

    def push_from_local(self, destination_filepath, local_filepath):
        samba = self.get_conn()
        if samba.exists(destination_filepath):
            if samba.isfile(destination_filepath):
                samba.remove(destination_filepath)
        else:
            folder = os.path.dirname(destination_filepath)
            if not samba.exists(folder):
                samba.mkdir(folder)
        samba.upload(local_filepath, destination_filepath)
