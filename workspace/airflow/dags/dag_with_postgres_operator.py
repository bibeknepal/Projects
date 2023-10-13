from datetime import datetime,timedelta

from airflow import DAG 
from airflow.providers.postgres.operators.postgres import PostgresOperator

defaut_args = {
    "retries":1,
    "retry_delay" : timedelta(minutes=5),
    'start_date': datetime(2023, 10, 12,11,0)
}

dag = DAG("dag_with_postgres_operator",default_args=defaut_args,schedule=None,catchup=False)

task1 = PostgresOperator(
    task_id = "create_table_dag_runs",
    postgres_conn_id= 'postgres_connection',
    sql = """
        create table if not exists dag_runs(
            dt date,
            dag_id character varying,
            primary key(dt,dag_id)
        )
            """,
    dag = dag
)
# task2 = PostgresOperator(
#     task_id = "insert_data_into_table",
#     postgres_conn_id= 'postgres_localhost',
#     sql = """
#            insert into dag_runs (dt,dag_id) values ('{{ds}}','{{dag.dag_id}}')
        
#             """,
#             dag = dag
#             )

task2 = PostgresOperator(
    task_id = "delete_data_from_table",
    postgres_conn_id= 'postgres_connection',
    sql = """
           delete from dag_runs where dt = '{{ds}}' and dag_id = '{{dag.dag_id}}'
        
            """,
            dag = dag
            )
task1 >> task2