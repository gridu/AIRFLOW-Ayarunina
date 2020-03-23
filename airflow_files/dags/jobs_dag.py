import logging
from datetime import datetime

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils.trigger_rule import TriggerRule

TriggerRule

config = {
    'query_table_1': {'schedule_interval': None, "start_date": datetime(2020, 3, 11), "table_name": "table_name_1"},
    'query_table_2': {'schedule_interval': None, "start_date": datetime(2018, 3, 11), "table_name": "table_name_1"},
    'query_table_3': {'schedule_interval': None, "start_date": datetime(2018, 3, 11), "table_name": "table_name_1"}}


def print_logs(database_name, table_name, ti, **context):
    logging.info("{} start processing table {} in database: {}".format(ti.dag_id,
                                                                       table_name,
                                                                       database_name))


def check_table_exist(true, false):
    """ method to check that table exists """
    if True:
        return true
    else:
        return false

def print_result(dag_run, **context):
    return "{} ended.".format(dag_run.run_id)


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

        execute_bash_task = BashOperator(task_id="execute_bash",
                                         bash_command='echo "$USER"',
                                         xcom_push=True)

        create_table_task = DummyOperator(task_id="create_table")
        dummy_task = DummyOperator(task_id="skip_table_creation")

        check_table_exist_task = BranchPythonOperator(task_id="check_table_exist",
                                                      python_callable=check_table_exist,
                                                      op_kwargs={"true": create_table_task.task_id,
                                                                 "false": dummy_task.task_id})
        insert_new_row_task = DummyOperator(task_id="insert_new_row",
                                            trigger_rule=TriggerRule.ALL_DONE)

        query_the_table_task = DummyOperator(task_id="query_the_table")

        print_result_task = PythonOperator(task_id="print_result",
                                           python_callable=print_result,
                                           provide_context=True)

        print_log_task \
        >> execute_bash_task \
        >> check_table_exist_task \
        >> [create_table_task, dummy_task] \
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
