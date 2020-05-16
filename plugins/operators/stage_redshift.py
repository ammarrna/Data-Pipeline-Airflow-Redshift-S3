from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    Redshift_Copy_Command="""
        COPY {} FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {} '{}';
        """    
    
    @apply_defaults
    def __init__(self,
                 Db_Connection="",
                 Table="",
                 Source_Bucket="",
                 Region="",
                 Data_format="",
                 Aws_Keys="",
                 Mode="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.Db_Connection = Db_Connection
        self.Table = Table
        self.Source_Bucket = Source_Bucket
        self.Region = Region
        self.Data_format = Data_format
        self.Aws_Keys = Aws_Keys
        self.Source_Bucket = Source_Bucket        
        self.Mode = Mode 
    def execute(self, context):
        self.log.info('Starting Stage Redshift Operator to copy:')
        AWS = AwsHook(self.Aws_Keys)
        Aws_Credential = AWS.get_credentials()
        Redshift_Connection = PostgresHook(postgres_conn_id=self.Db_Connection)    
        Redshift_Connection.run("DELETE FROM {}".format(self.Table))
        Copy_Sql_Command = StageToRedshiftOperator.Redshift_Copy_Command.format(
                self.Table, 
                self.Source_Bucket, 
                Aws_Credential.access_key,
                Aws_Credential.secret_key, 
                self.Region,
                self.Data_format,
                self.Mode)        
        Redshift_Connection.run(Copy_Sql_Command)



