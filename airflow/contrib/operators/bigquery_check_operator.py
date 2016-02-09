from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators import CheckOperator, ValueCheckOperator, IntervalCheckOperator
from airflow.utils import apply_defaults


class BigQueryCheckOperator(CheckOperator):
    # TODO pydocs
    @apply_defaults
    def __init__(
            self,
            sql,
            bigquery_conn_id='bigquery_default',
            *args,
            **kwargs):
        super(BigQueryCheckOperator, self).__init__(sql=sql, *args, **kwargs)
        self.bigquery_conn_id = bigquery_conn_id
        self.sql = sql

    def get_db_hook(self):
        return BigQueryHook(bigquery_conn_id=self.bigquery_conn_id)

class BigQueryValueCheckOperator(ValueCheckOperator):
    # TODO pydocs

    @apply_defaults
    def __init__(
            self, sql, pass_value, tolerance=None,
            bigquery_conn_id='bigquery_default',
            *args, **kwargs):
        super(BigQueryValueCheckOperator, self).__init__(sql=sql, pass_value=pass_value, tolerance=tolerance, *args, **kwargs)
        self.bigquery_conn_id = bigquery_conn_id

    def get_db_hook(self):
        return BigQueryHook(bigquery_conn_id=self.bigquery_conn_id)


class BigQueryIntervalCheckOperator(IntervalCheckOperator):
    # TODO pydocs

    @apply_defaults
    def __init__(
            self, table, metrics_thresholds,
            date_filter_column='ds', days_back=-7,
            bigquery_conn_id='bigquery_default',
            *args, **kwargs):
        super(BigQueryIntervalCheckOperator, self).__init__(
            table=table, metrics_thresholds=metrics_thresholds, date_filter_column=date_filter_column, days_back=days_back,
            *args, **kwargs)
        self.bigquery_conn_id = bigquery_conn_id

    def get_db_hook(self):
        return BigQueryHook(bigquery_conn_id=self.bigquery_conn_id)
