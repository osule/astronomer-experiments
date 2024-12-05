from airflow.decorators import dag, task_group
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator, SQLExecuteQueryOperator
)

import pendulum

default_args = {
    "owner": "Oluwafemi Sule",
}

@dag(
    default_args=default_args,
    schedule_interval="0 0 * * *",  # Cron schedule
    start_date=pendulum.datetime(2024, 12, 4, tz="UTC"),
    catchup=False,
    owner_links={
        "Oluwafemi Sule": "https://github.com/osule/astronomer-experiments",
    },
)
def expand_tasks_sql_orig():
    @task_group(group_id="group1")
    def tg1(id):
        # Task 1: Check if the condition is met
        expanded_params = {"id": id}
        check1 = SQLCheckOperator(
            task_id='check1',
            sql='SELECT count(*) > 0 FROM data_table WHERE id = {{ params.id.resolve({"ti": ti, "run_id": run_id}) }}',
            params=expanded_params,
            conn_id="!!TODO: Set this"
        )

        # Task 2: Update the table if condition is met
        update1 = SQLExecuteQueryOperator(
            task_id='update1',
            sql='''UPDATE table SET date = NOW() WHERE id = {{ params.id.resolve({"ti": ti, "run_id": run_id}) }}''',
            params=expanded_params,
            conn_id="!!TODO: Set this"
        )
        
        # Set task dependencies
        check1 >> update1

    # Create 6 instances of the task group group1 with dynamic task mapping
    tg1.expand(id=[19, 23, 42, 8, 7, 108])

dag_obj = expand_tasks_sql_orig()