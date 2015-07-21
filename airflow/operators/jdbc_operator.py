__author__ = 'janomar'

import logging

from airflow.hooks.jdbc_hook import JdbcHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class JdbcOperator(BaseOperator):
    """
    Executes sql code in a database using jdbc driver.

    Requires jaydebeapi.

    :param jdbc_url: driver specific connection url with string variables, e.g. for exasol jdbc:exa:{0}:{1};schema={2}
    Template vars are defined like this: {0} = hostname, {1} = port, {2} = dbschema, {3} = extra
    :type jdbc_url: string
    :param jdbc_driver_name: classname of the specific jdbc driver, for exasol com.exasol.jdbc.EXADriver
    :type jdbc_driver_name: string
    :param jdbc_driver_loc: absolute path to jdbc driver location, for example /var/exasol/exajdbc.jar
    :type jdbc_driver_loc: string

    :param conn_id: reference to a predefined database
    :type conn_id: string
    :param sql: the sql code to be executed
    :type sql: string or string pointing to a template file. File must have
        a '.sql' extensions.
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql,
            jdbc_url, jdbc_driver_name, jdbc_driver_loc,
            conn_id='jdbc_default', autocommit=False,
            *args, **kwargs):
        super(JdbcOperator, self).__init__(*args, **kwargs)

        self.jdbc_url=jdbc_url
        self.jdbc_driver_name=jdbc_driver_name
        self.jdbc_driver_loc=jdbc_driver_loc
        self.sql = sql
        self.conn_id = conn_id
        self.autocommit = autocommit

    def execute(self, context):
        logging.info('Executing: ' + self.sql)
        self.hook = JdbcHook(conn_id=self.conn_id,jdbc_driver_loc=self.jdbc_driver_loc, jdbc_driver_name=self.jdbc_driver_name,jdbc_url=self.jdbc_url)
        for row in self.hook.get_records(self.sql, self.autocommit):
            logging.info('Result: ' + ','.join(map(str,row)) )