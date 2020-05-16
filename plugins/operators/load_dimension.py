from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 Db_Connection="",
                 Table="",
                 AppendMode=False,
                 sql_statement="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.Db_Connection=Db_Connection
        self.Table=Table
        self.AppendMode=AppendMode
        self.sql_statement=sql_statement


    def execute(self, context):
        self.log.info('LoadDimensionOperator')
        redshift_cur = PostgresHook(postgres_conn_id=self.Db_Connection)
        self.log.info('Starting load for Table {}'.format(self.Table))
        if self.AppendMode:
            self.log.info('Appending rows')
        else:
            self.log.info('Deleting rows')
            redshift_cur.run("DELETE FROM {}".format(self.Table))
        formatted_sql = getattr(SqlQueries,self.sql_statement).format(self.Table)
        redshift_cur.run(formatted_sql)
        self.log.info('Completed load for Table {}'.format(self.Table))