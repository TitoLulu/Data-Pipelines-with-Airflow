from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region '{}'
        json '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id ='',
                 aws_cred_id='',
                 target_table ='',
                 s3_path = '',
                 s3_region ='',
                 json_path='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Params mapping
        self.redshift_conn_id = redshift_conn_id
        self.aws_cred_id = aws_cred_id
        self.target_table = target_table
        self.s3_path = s3_path
        self.json_path = json_path
        self.s3_region = s3_region
        

    def execute(self, context):
        self.log.info('copy s3 files to staging tables')
        aws_hook = AwsHook(self.aws_cred_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        sql=StageToRedshiftOperator.copy_sql.format(
            self.target_table,
            self.s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.s3_region,
            self.json_path,
            
        )
        
        redshift.run(sql)
        





