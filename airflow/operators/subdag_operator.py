from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class SubDagOperator(BaseOperator):

    template_fields = tuple()
    ui_color = '#555'
    ui_fgcolor = '#fff'

    __mapper_args__ = {
        'polymorphic_identity': 'SubDagOperator'
    }

    @apply_defaults
    def __init__(
            self,
            subdag,
            *args, **kwargs):
        """
        Yo dawg. This runs a sub dag. By convention, a sub dag's dag_id
        should be prefixed by its parent and a dot. As in `parent.child`.

        :param subdag: the DAG object to run as a subdag of the current DAG.
        :type subdag: airflow.DAG
        :param dag: the parent DAG
        :type subdag: airflow.DAG
        """
        if 'dag' not in kwargs:
            raise Exception("Please pass in the `dag` param")
        dag = kwargs['dag']
        super(SubDagOperator, self).__init__(*args, **kwargs)
        if dag.dag_id + '.' + kwargs['task_id'] != subdag.dag_id:
            raise Exception(
                "The subdag's dag_id should correspond to the parent's "
                "'dag_id.task_id'")
        self.subdag = subdag

    def execute(self, context):
        ed = context['execution_date']
        self.subdag.run(start_date=ed, end_date=ed, donot_pickle=True)
