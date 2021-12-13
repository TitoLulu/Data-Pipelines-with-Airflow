from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    sql_insert = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id ='',
                 dim_table ='',
                 load_dim_sql='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id  = redshift_conn_id
        self.dim_table = dim_table
        self.load_dim_sql = load_dim_sql

    def execute(self, context):
        self.log.info('LoadDimensionOperator implementation')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql = LoadDimensionOperator.sql_insert.format(
            self.dim_table,
            self.load_dim_sql,
        )
        redshift.run(sql)
            
        
        
