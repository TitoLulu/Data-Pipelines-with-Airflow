from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)
    copy_sql = """
        copy {}
        from '{}'
        access_key_id '{}'
        access_key_secret '{}'
        json_path '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id ='',
                 aws_cred_id='',
                 target_table ='',
                 s3_bucket ='',
                 s3_key='',
                 json_path='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Params mapping
        self.redshift_conn_id = redshift_conn_id
        self.aws_cred_id = aws_cred_id
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        

    def execute(self, context):
        self.log.info('copy s3 files to staging tables')
        aws_hook = AWSHOOK(self.aws_cred_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket,rendered_key)
        
        sql=StageToRedshiftOperator.copy_sql.format(
            self.target_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )
        
        redshift.run(sql)
        





