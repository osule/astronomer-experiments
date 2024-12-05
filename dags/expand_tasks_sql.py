from airflow.decorators import dag, task_group
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator



import pendulum

# Default arguments for the DAG
default_args = {
    "owner": "Oluwafemi Sule",
}

@dag(
    default_args=default_args,
    schedule_interval="0 0 * * *",  # Cron schedule
    start_date=pendulum.datetime(2024, 12, 4, tz="UTC"),
    catchup=False,
    owner_links={
        "Oluwafemi Sule": "https://cloud.astronomer.io/cm1eu47w413zr01nqex144eth/cloud-ide/cm1eu73d3140701nqx7ces5cz/cm49ulf811oxj01o6bu4tsaxp",
    },
)
def expand_tasks_sql():
    """A DAG demonstrating task group expansion."""

    @task_group(group_id="group1")
    def tg1(id):
        """Task group to process a number."""
        
        # Task to print the number
        check1 = SQLCheckOperator(
            task_id="check1",
            sql='SELECT {{ params.id.resolve({"ti": ti, "run_id": run_id}) }} > 1',
            params={"id": id},  # Pass as an environment variable for dynamic expansion
            conn_id="default"
        )

        # Task to add 42 to the number
        update1 = SQLExecuteQueryOperator(
            task_id="update1",
            sql='SELECT 1 + {{ params.id.resolve({"ti": ti, "run_id": run_id}) }}',
            params={"id": id},
            conn_id="default"
        )

        # Set task dependencies
        check1 >> update1

    # Expanding the task group with a list of input values
    tg1.expand(id=[19, 23, 42, 8, 7, 108])

# Instantiate the DAG
dag_obj = expand_tasks_sql()
