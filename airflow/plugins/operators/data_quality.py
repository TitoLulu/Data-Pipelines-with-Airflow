from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    load_check = """select count(*) from {}"""

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table =[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        self.log.info('DataQualityOperator implentation')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for t in self.table:
            self.table = t
            count=redshift.get_records(DataQualityOperator.load_check.format(t))
            print(type(count))
            if not count:
                self.log.info(f'{t} is empty')
            else:
                self.log.info('load complete')
            