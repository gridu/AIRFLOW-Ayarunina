# This is the class you derive to create a plugin
# from airflow.operators.postgres_custom import PostgreSQLCountRows """
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


class PostgreSQLCountRows(BaseOperator):
    """ Count rows """

    @apply_defaults
    def __init__(self, table_name,
                 *args, **kwargs):
        """
    
        :param table_name: table name
        """
        self.table_name = table_name
        self.hook = PostgresHook()
        super(PostgreSQLCountRows, self).__init__(*args, **kwargs)

    def execute(self, context):
        count = self.hook.get_first(sql="SELECT COUNT(*) FROM {};".format(self.table_name))
        logging.info("Count: {}".format(count))
        return count


# Defining the plugin class
class AirflowTestPlugin(AirflowPlugin):
    name = "postgres_custom"
    operators = [PostgreSQLCountRows]
