import logging
import tempfile

from airflow.hooks import HiveServer2Hook, SambaHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class Hive2SambaOperator(BaseOperator):
    """
    Executes hql code in a specific Hive database.

    :param hql: the hql to be exported
    :type hql: string
    :param hiveserver2_conn_id: reference to the hiveserver2 service
    :type hiveserver2_conn_id: string
    :param samba_conn_id: reference to the samba destination
    :type samba_conn_id: string
    """

    __mapper_args__ = {
        'polymorphic_identity': 'Hive2SambaOperator'
    }
    template_fields = ('hql', 'destination_filepath')
    template_ext = ('.hql', '.sql',)

    @apply_defaults
    def __init__(
            self, hql,
            destination_filepath,
            samba_conn_id='samba_default',
            hiveserver2_conn_id='hiveserver2_default',
            *args, **kwargs):
        super(Hive2SambaOperator, self).__init__(*args, **kwargs)

        self.hiveserver2_conn_id = hiveserver2_conn_id
        self.samba_conn_id = samba_conn_id
        self.destination_filepath = destination_filepath
        self.samba = SambaHook(samba_conn_id=samba_conn_id)
        self.hook = HiveServer2Hook(hiveserver2_conn_id=hiveserver2_conn_id)
        self.hql = hql.strip().rstrip(';')

    def execute(self, context):
        tmpfile = tempfile.NamedTemporaryFile()
        self.hook.to_csv(hql=self.hql, csv_filepath=tmpfile.name)
        self.samba.push_from_local(self.destination_filepath, tmpfile.name)
