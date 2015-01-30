import logging

from airflow.configuration import conf
from airflow.hooks import PrestoHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class PrestoCheckOperator(BaseOperator):
    """
    Performs a simple check using sql code in a specific Presto database.

    :param sql: the sql to be executed
    :type sql: string
    :param presto_dbid: reference to the Presto database
    :type presto_dbid: string
    """

    __mapper_args__ = {
        'polymorphic_identity': 'PrestoCheckOperator'
    }
    template_fields = ('sql',)
    template_ext = ('.hql', '.sql',)

    @apply_defaults
    def __init__(
            self, sql,
            presto_conn_id=conf.get('hooks', 'PRESTO_DEFAULT_CONN_ID'),
            *args, **kwargs):
        super(PrestoCheckOperator, self).__init__(*args, **kwargs)

        self.presto_conn_id = presto_conn_id
        self.hook = PrestoHook(presto_conn_id=presto_conn_id)
        self.sql = sql

    def execute(self, execution_date=None):
        logging.info('Executing SQL check: ' + self.sql)
        records = self.hook.get_first(hql=self.sql)
        logging.info("Record: " + str(records))
        if not records:
            raise Exception("The query returned None")
        elif not all([bool(r) for r in records]):
            exceptstr = "Test failed.\nQuery:\n{q}\nResults:\n{r!s}"
            raise Exception(exceptstr.format(q=self.sql, r=records))
        logging.info("Success.")


class PrestoIntervalCheckOperator(BaseOperator):
    """
    Checks that the values of metrics given as SQL expressions are within
    a certain tolerance of the ones from days_back before.

    :param table: the table name
    :type table: str
    :param days_back: number of days between ds and the ds we want to check
        against. Defaults to 7 days
    :type days_back: int
    :param metrics_threshold: a dictionary of ratios indexed by metrics
    :type metrics_threshold: dict
    :param presto_dbid: reference to the Presto database
    :type presto_dbid: string
    """

    __mapper_args__ = {
        'polymorphic_identity': 'PrestoIntervalCheckOperator'
    }
    template_fields = ('sql1','sql2')
    template_ext = ('.hql', '.sql',)

    @apply_defaults
    def __init__(
            self, table, metrics_thresholds,
            date_filter_column='ds', days_back=-7,
            presto_conn_id=conf.get('hooks', 'PRESTO_DEFAULT_CONN_ID'),
            *args, **kwargs):
        super(PrestoIntervalCheckOperator, self).__init__(*args, **kwargs)
        self.presto_conn_id = presto_conn_id
        self.hook = PrestoHook(presto_conn_id=presto_conn_id)
        self.table = table
        self.metrics_thresholds = metrics_thresholds
        self.metrics_sorted = sorted(metrics_thresholds.keys())
        self.date_filter_column = date_filter_column
        self.days_back = -abs(days_back)
        sqlexp = ', '.join(self.metrics_sorted)
        sqlt = ("SELECT {sqlexp} FROM {table}"
                " WHERE {date_filter_column}=").format(**locals())
        self.sql1 = sqlt + "'{{ ds }}'"
        self.sql2 = sqlt + "'{{ macros.ds_add(ds, "+str(self.days_back)+") }}'"

    def execute(self, execution_date=None):
        logging.info('Executing SQL check: ' + self.sql2)
        row2 = self.hook.get_first(hql=self.sql2)
        logging.info('Executing SQL check: ' + self.sql1)
        row1 = self.hook.get_first(hql=self.sql1)
        if not row2:
            raise Exception("The query {q} returned None").format(q=self.sql2)
        if not row1:
            raise Exception("The query {q} returned None").format(q=self.sql1)
        current = dict(zip(self.metrics_sorted, row1))
        reference = dict(zip(self.metrics_sorted, row2))
        ratios = {}
        test_results = {}
        rlog = "Ratio for {0}: {1} \n Ratio threshold : {2}"
        fstr = "'{k}' check failed. {r} is above {tr}"
        estr = "The following tests have failed:\n {0}"
        countstr = "The following {j} tests out of {n} failed:"
        for m in self.metrics_sorted:
            if current[m] == 0 or reference[m] == 0:
                ratio = None
            else:
                ratio = float(max(current[m], reference[m])) / \
                    min(current[m], reference[m])
            logging.info(rlog.format(m, ratio, self.metrics_thresholds[m]))
            ratios[m] = ratio
            test_results[m] = ratio < self.metrics_thresholds[m]
        if not all(test_results.values()):
            failed_tests = [it[0] for it in test_results.items() if not it[1]]
            j = len(failed_tests)
            n = len(self.metrics_sorted)
            logging.warning(countstr.format(**locals()))
            for k in failed_tests:
                logging.warning(fstr.format(k=k, r=ratios[k],
                                tr=self.metrics_thresholds[k]))
            raise Exception(estr.format(", ".join(failed_tests)))
        logging.info("All tests have passed")
