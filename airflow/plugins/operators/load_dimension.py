from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id ='',
                 load_dim_sql ='',
                 table = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id  = redshift_conn_id
        self.load_dim_sql = load_dim_sql
        self.table = table

    def execute(self, context):
        self.log.info('LoadDimensionOperator implementation')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql = LoadDimensionOperator.load_dim_sql.format(
            self.table,
        )
        redshift.run(sql)
            
        
        
