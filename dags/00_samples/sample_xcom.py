from airflow.sdk import dag, get_current_context, task
from airflow.utils.state import DagRunState
from datetime import datetime
import time
import datetime as dt
from airflow.sdk import Variable
from airflow.exceptions import AirflowSkipException


@dag(dag_id="example_dag", start_date=datetime(2025, 1, 1), schedule="@hourly", tags=["demo"], catchup=False)
def example_dag():

    @task(task_id="compute_time_infos")
    def compute_time_infos():
        if time.time() - float(Variable.get("example_dag_start_time", 0)) < 60:
            raise AirflowSkipException("dont run too often")
        Variable.set("example_dag_start_time", str(time.time()))

        utcnow = dt.datetime.now(dt.UTC)
        return {
            "timestamp": time.perf_counter(),
            "datetime": utcnow,
            "iso": utcnow.isoformat("_", "milliseconds"),
        }

    @task(task_id="display_time")
    def display_time(date_infos):
        print("dag started at", date_infos["iso"])

    @task(task_id="display_elapsed")
    def elapsed(date_infos):
        print("Elapsed time since first task :", time.perf_counter() - date_infos["timestamp"])

    start_info = compute_time_infos()
    display_time(start_info)
    elapsed(start_info)

example_dag()
