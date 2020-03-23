import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import XCom
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

FILEPATH = "/Users/ayarunina/airflow/run"
FINISH_FILEPATH_PREFIX = "/Users/ayarunina/airflow/finished"
EXTERNAL_DAG_ID = "query_table_1"
EXTERNAL_TASK_ID = "print_result"


def print_result(external_dag_id, external_task_id, ti, **context):
    result = ti.xcom_pull(task_ids=external_task_id,
                          dag_id=external_dag_id)
    logging.info(result)
    for key in context:
        logging.info("key: {}, value:{}".format(key, context[key]))


def process_result_subdag(parent_dag_name,
                          child_dag_name,
                          schedule_interval,
                          start_date,
                          external_dag_id,
                          external_task_id):
    with DAG(dag_id='%s.%s' % (parent_dag_name, child_dag_name),
             schedule_interval=schedule_interval,
             start_date=start_date,
             ) as dag:
        sensor_triggered_dag_task = ExternalTaskSensor(task_id="sensor_triggered_dag",
                                                       external_dag_id=external_dag_id,
                                                       execution_delta=timedelta(minutes=0))

        print_result_task = PythonOperator(task_id="print_result",
                                           python_callable=print_result,
                                           op_args=(external_dag_id, external_task_id),
                                           provide_context=True)

        remove_run_file_task = BashOperator(task_id="remove_run_file",
                                            bash_command="rm {}".format(FILEPATH))

        create_finished_file_task = BashOperator(task_id="create_finished_file",
                                                 bash_command="touch %s_{{ts_nodash}}" % FINISH_FILEPATH_PREFIX)

        sensor_triggered_dag_task >> \
        print_result_task >> \
        remove_run_file_task >> \
        create_finished_file_task
        return dag


with DAG("trigger-dag",
         schedule_interval=None,
         start_date=datetime(2020, 1, 1),
         catchup=False) as dag:
    wait_file_task = FileSensor(task_id="wait_file",
                                filepath=FILEPATH)

    trigger_dag_task = TriggerDagRunOperator(task_id="trigger_dag",
                                             trigger_dag_id="query_table_1",
                                             execution_date="{{ execution_date }}")

    process_result_subdag_task = SubDagOperator(task_id="process_result_subdag",
                                                subdag=process_result_subdag(dag.dag_id,
                                                                             "process_result_subdag",
                                                                             dag.schedule_interval,
                                                                             dag.start_date,
                                                                             EXTERNAL_DAG_ID,
                                                                             EXTERNAL_TASK_ID))

    wait_file_task >> trigger_dag_task >> process_result_subdag_task
