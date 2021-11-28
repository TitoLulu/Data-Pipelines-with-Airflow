from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 load_fact_sql ='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_fact_sql = load_fact_sql

    def execute(self, context):
        self.log.info('LoadFactOperator implementation')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql = LoadFactOperator.load_fact_sql.format(
            self.table,
        )
        redshift.run(sql)
        
