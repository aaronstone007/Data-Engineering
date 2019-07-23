from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    # Insert to dimension tables
    insert_sql="""
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 table = "",
                 select_sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table=table
        self.redshift_conn_id = redshift_conn_id    
        self.select_sql = select_sql

    def execute(self, context):
        """
        1. Fetch connections for redshift 
        2. Format sql
        """
        self.log.info('LoadDimensionOperator initiated')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)        
        self.log.info('Loading Dimension Tables')
        formatted_insert_sql = LoadDimensionOperator.insert_sql.format(
                self.table,
                self.select_sql
            )
        redshift.run(formatted_insert_sql)
        self.log.info('Done')
