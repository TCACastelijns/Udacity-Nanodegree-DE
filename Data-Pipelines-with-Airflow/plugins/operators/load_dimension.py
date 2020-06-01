from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 truncate = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate = truncate
        

    def execute(self, context):
        """
        Method to insert data into dimensional tables from the staging phase (events and songs).
        """
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run(f"TRUNCATE TABLE {self.table}")
                
        self.log.info("Inserting data into destination Redshift table")
        redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
