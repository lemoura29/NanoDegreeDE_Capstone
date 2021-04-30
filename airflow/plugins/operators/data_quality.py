from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    data_quality_checks = [
        {'check_sql' : "SELECT COUNT(*) FROM dim_country WHERE country_code IS NULL",
         'expected_result' : 0,
         'table': "dim_country"},
        {'check_sql' : "SELECT COUNT(*) FROM dim_indicator WHERE code IS NULL",
         'expected_result' : 0,
         'table': "dim_indicator"},
        {'check_sql' : "SELECT COUNT(*) FROM fact_score WHERE country_code IS NULL",
         'expected_result' : 0,
         'table': "fact_score"},
        {'check_sql' : "SELECT COUNT(*) FROM fact_score WHERE indicator_code IS NULL",
         'expected_result' : 0,
         'table': "fact_score"}]

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Start assessment of the data quality of the tables.')
        
        for check in DataQualityOperator.data_quality_checks:
            sql_query = check.get('check_sql')
            exp_result = check.get('expected_result')
            table = check.get('table')
            
            
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
      
            num_records = records[0][0]
        
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")

            self.log.info(f'Starting data quality null check for {table} in Redshift')
            records = redshift_hook.get_records(sql_query)[0]
            
            error_count = 0
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)
                
            if error_count > 0:
                self.log.info(f'Tests failed on table: {table}')
                self.log.info(failing_tests)
                raise ValueError('Data quality check failed')
            
            
            