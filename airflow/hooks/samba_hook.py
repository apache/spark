import os
from smbclient import SambaClient

from airflow import settings
from airflow.models import Connection


class SambaHook(object):
    '''
    Allows for interaction with an samba server.
    '''

    def __init__(self, samba_conn_id=None):
        session = settings.Session()
        samba_conn = session.query(
            Connection).filter(
                Connection.conn_id == samba_conn_id).first()
        if not samba_conn:
            raise Exception("The samba id you provided isn't defined")
        self.host = samba_conn.host
        self.login = samba_conn.login
        self.psw = samba_conn.password
        self.db = samba_conn.schema
        session.commit()
        session.close()

    def get_conn(self):
        samba = SambaClient(
            server=self.host, share='', username=self.login, password=self.psw)
        return samba

    def push_from_local(self, destination_filepath, local_filepath):
        samba = self.get_conn()
        samba.cwd(os.path.dirname(self.destination_filepath))
        f = open(local_filepath, 'r')
        filename = os.path.basename(destination_filepath)
        samba.storbinary("STOR " + filename, f)
        f.close()
        samba.quit()
