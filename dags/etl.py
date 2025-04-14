from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago

# Define the DAG
with DAG(
    dag_id='nasa_apod_postgres',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    tags=["nasa", "postgres"]
) as dag:

    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        postgres_hook.run(create_table_query)

    # Extract data from NASA API using SimpleHttpOperator
    extract_apod = SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',
        endpoint='planetary/apod',
        method='GET',
        data={"api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"},
        response_filter=lambda response: response.json()
    )

    @task
    def transform_apod_data(response):
        return {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }

    @task
    def load_data_to_postgres(apod_data):
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))

    # Set task dependencies
    table_task = create_table()
    transformed_data = transform_apod_data(extract_apod.output)
    
    table_task >> extract_apod >> transformed_data >> load_data_to_postgres(transformed_data)
