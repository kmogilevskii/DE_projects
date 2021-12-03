from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql='',
                 operation='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.operation = operation

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.operation == 'delete':
            self.log.info(f"Deleting data from the {self.table} table.")
            redshift.run(f"TRUNCATE {self.table}")
        
        self.log.info(f"Loading data into the {self.table} table.")
        redshift.run("INSERT INTO {} {}".format(self.table, self.sql))
