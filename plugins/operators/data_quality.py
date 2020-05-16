from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 Db_Connection="",
                 Table="",
                 TotalRecords=0,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.Db_Connection=Db_Connection
        self.Table=Table
        self.TotalRecords=TotalRecords

    def execute(self, context):
        self.log.info('DataQualityOperator')
        redshift_hook = PostgresHook(postgres_conn_id=self.Db_Connection)
        records = redshift_hook.get_records("SELECT COUNT(*) FROM songs")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError("Data quality check failed. {} returned no results".format(self.Table))
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError("Data quality check failed. {} contained 0 rows".format(self.Table))
        self.log.info("Data quality on table {} check passed".format(self.Table))
       
       