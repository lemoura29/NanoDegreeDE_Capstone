from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    truncate_sql = """
                    TRUNCATE TABLE {};
                   """
    insert_sql = """
                    INSERT INTO {}({}) {};
                 """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 table = "",
                 truncate_data = True,
                 sql_query = "",
                 columns ="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate_data = truncate_data
        self.sql_query = sql_query
        self.columns = columns
        

    def execute(self, context):
        self.log.info(f"Started execution on {self.table}.")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f" Delete existing data table")
        if self.truncate_data:
            redshift_hook.run(LoadFactOperator.truncate_sql.format(self.table))
        
        redshift_hook.run(LoadFactOperator.insert_sql.format(self.table,self.columns, self.sql_query))
        
        self.log.info(f"Completed execution on {self.table}")
