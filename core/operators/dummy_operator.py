from core.models import BaseOperator


class DummyOperator(BaseOperator):

    template_fields = tuple()

    __mapper_args__ = {
        'polymorphic_identity': 'DummyOperator'
    }

    def __init__(self, *args, **kwargs):
        super(DummyOperator, self).__init__(*args, **kwargs)

    def execute(self, execution_date):
        pass
