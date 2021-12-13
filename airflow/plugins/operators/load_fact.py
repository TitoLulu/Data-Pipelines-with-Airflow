from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    sql_insert = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 fact_table ='',
                 load_fact_sql ='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table = fact_table
        self.load_fact_sql = load_fact_sql

    def execute(self, context):
        self.log.info('LoadFactOperator implementation')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql = LoadFactOperator.sql_insert.format(
            self.fact_table,
            self.load_fact_sql,
        )
        redshift.run(sql)
        
