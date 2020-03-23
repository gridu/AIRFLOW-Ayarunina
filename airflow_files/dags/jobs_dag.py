import logging
import uuid
from datetime import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_custom import PostgreSQLCountRows
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

config = {
    'query_table_1': {'schedule_interval': None, "start_date": datetime(2020, 3, 11), "table_name": "table_name_1"},
    'query_table_2': {'schedule_interval': None, "start_date": datetime(2018, 3, 11), "table_name": "table_name_1"},
    'query_table_3': {'schedule_interval': None, "start_date": datetime(2018, 3, 11), "table_name": "table_name_1"}}


def print_logs(database_name, table_name, ti, **context):
    logging.info("{} start processing table {} in database: {}".format(ti.dag_id,
                                                                       table_name,
                                                                       database_name))


def check_table_exist(table_name, true, false):
    """ callable function to get schema name and after that check if table exist """
    global schema
    hook = PostgresHook()
    # get schema name
    sql_to_get_schema = "SELECT * FROM pg_tables;"
    query = hook.get_records(sql=sql_to_get_schema)
    for result in query:
        logging.info(result)
        if 'airflow' in result:
            schema = result[0]
            logging.info(schema)
            break

    # check table exist
    sql_to_check_table_exist = "SELECT * FROM information_schema.tables " + \
                               "WHERE table_schema = '{}'" + \
                               "AND table_name = '{}'"
    query = hook.get_first(sql=sql_to_check_table_exist.format(schema, table_name))
    logging.info(query)
    if query:
        return true
    else:
        logging.info("table {} does not exist".format(table_name))
        return false


def query_the_table(table_name, **context):
    hook = PostgresHook()
    return hook.get_first("SELECT COUNT(*) FROM {};".format(table_name))


def print_result(dag_run, **context):
    return "{} ended.".format(dag_run.run_id)


def test(ti, **context):
    logging.info(ti.xcom_pull(task_ids="print_user"))
    logging.info(ti.xcom_pull(task_ids="print_user", key="return_value"))


def create_dag(dag_id,
               schedule_interval,
               start_date,
               database_name,
               table_name):
    with DAG(dag_id,
             schedule_interval=schedule_interval,
             start_date=start_date,
             catchup=False) as dag:
        print_log_task = PythonOperator(task_id="print_log",
                                        python_callable=print_logs,
                                        op_args=(database_name, table_name),
                                        provide_context=True)

        print_user_task = BashOperator(task_id="print_user",
                                       bash_command='echo "airflow"',
                                       xcom_push=True)

        create_table_task = PostgresOperator(task_id="create_table",
                                             sql='''CREATE TABLE {}(custom_id integer NOT NULL,
                                                 user_name VARCHAR (50) NOT NULL, timestamp TIMESTAMP NOT NULL);'''.format(
                                                 table_name))

        dummy_task = DummyOperator(task_id="skip_table_creation")

        python_task = PythonOperator(task_id="test",
                                     python_callable=test,
                                     provide_context=True,
                                     trigger_rule=TriggerRule.ALL_DONE)

        check_table_exist_task = BranchPythonOperator(task_id="check_table_exist",
                                                      python_callable=check_table_exist,
                                                      op_kwargs={"table_name": table_name,
                                                                 "true": dummy_task.task_id,
                                                                 "false": create_table_task.task_id})
        insert_new_row_task = PostgresOperator(task_id="insert_new_row",
                                               sql='''INSERT INTO {} VALUES(%s, '{{{{ ti.xcom_pull('print_user') }}}}', %s);'''.format(
                                                   table_name),
                                               parameters=(uuid.uuid4().int % 123456789,
                                                           datetime.now()),
                                               trigger_rule=TriggerRule.ALL_DONE)

        query_the_table_task = PostgreSQLCountRows(task_id="query_the_table",
                                                   table_name=table_name)

        print_result_task = PythonOperator(task_id="print_result",
                                           python_callable=print_result,
                                           provide_context=True)

        print_log_task \
        >> print_user_task \
        >> check_table_exist_task \
        >> [create_table_task, dummy_task] \
        >> python_task \
        >> insert_new_row_task \
        >> query_the_table_task \
        >> print_result_task
    return dag


for dag_id in config:
    dag_config = config[dag_id]
    globals()[dag_id] = create_dag(dag_id,
                                   dag_config["schedule_interval"],
                                   dag_config["start_date"],
                                   "database",
                                   dag_config["table_name"])

# if __name__ == '__main__':
#     print("test")
