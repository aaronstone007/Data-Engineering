from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    # for S3 key
    template_fields = ("s3_key",)
    # if file type is json
    json_copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}'
    """
    # if file type is csv
    csv_copy_sql ="""
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="",
                 file_type="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):
        

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path=json_path
        self.file_type=file_type
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        """
        1. Fetch connections for S3  and redshift
        2. Check file_type and load appropriate sql template
        """
        self.log.info('StageToRedshiftOperator initiated')
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(s3_path)
        if self.file_type=='json':
            formatted_json_copy_sql = StageToRedshiftOperator.json_copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_path
            )
            redshift.run(formatted_json_copy_sql)
            
        if self.file_type=='csv':
            
            formatted_csv_copy_sql = StageToRedshiftOperator.csv_copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
            )
            redshift.run(formatted_csv_copy_sql)
            
        self.log.info("Copying data from S3 to Redshift")
            






