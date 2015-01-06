import ftplib
import os

from airflow import settings
from airflow.models import DatabaseConnection


class FtpHook(object):
    '''
    Allows for interaction with an ftp server.
    '''

    def __init__(self, ftp_dbid=None):
        session = settings.Session()
        ftp_conn = session.query(
            DatabaseConnection).filter(
                DatabaseConnection.db_id == ftp_dbid).first()
        if not ftp_conn:
            raise Exception("The ftp id you provided isn't defined")
        self.host = ftp_conn.host
        self.port = ftp_conn.port or 21
        self.login = ftp_conn.login
        self.psw = ftp_conn.password
        self.db = ftp_conn.schema
        session.commit()
        session.close()

    def get_conn(self):
        ftp = ftplib.FTP()
        ftp.connect(self.host, self.port)
        ftp.login(self.login, self.psw)
        return ftp

    def push_from_local(self, destination_filepath, local_filepath):
        ftp = self.get_conn()
        ftp.cwd(os.path.dirname(self.destination_filepath))
        f = open(local_filepath, 'r')
        filename = os.path.basename(destination_filepath)
        ftp.storbinary("STOR " + filename, f)
        f.close()
        ftp.quit()
