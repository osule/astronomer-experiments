from airflow.decorators import dag, task_group, task
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
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
def expand_tasks_sql_hack_1():
    @task_group(group_id="group1")
    def tg1(id):
        # Task 1: Check if the condition is met
        @task
        def check1(id, **context):
            expanded_params = {"id": id}
            context["params"] = expanded_params
            operator = SQLCheckOperator(
                task_id="check1",
                sql="SELECT count(*) > 0 FROM data_table WHERE id = {{ params.id }}",
                conn_id="!!TODO: Set this",
            )
            operator.render_template_fields(context=context)
            return operator.execute(context=context)

        # Task 2: Update the table if condition is met
        @task
        def update1(id, **context):
            expanded_params = {"id": id}
            context["params"] = expanded_params
            operator = SQLExecuteQueryOperator(
                task_id="update1",
                sql="""UPDATE table SET date = NOW() WHERE id = {{ params.id }}""",
                conn_id="!!TODO: Set this",
            )
            operator.render_template_fields(context=context)
            return operator.execute(context=context)

        # Set task dependencies
        check1(id) >> update1(id)

    # Create 6 instances of the task group group1 with dynamic task mapping
    tg1.expand(id=[19, 23, 42, 8, 7, 108])


dag_obj = expand_tasks_sql_hack_1()
