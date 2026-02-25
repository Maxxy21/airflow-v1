from airflow.sdk import dag, task


@dag(dag_id="parallel_dags")
def parallel_dags():
    @task.python
    def extract_task(**kwargs):
        print("Extracting data from the source...")
        ti = kwargs["ti"]
        extracted_data_dict = {"api_extracted_data": [1, 2, 3, 4, 5],
                               "db_extracted_data": [6, 7, 8, 9, 10],
                               "s3_extracted_data": [11, 12, 13, 14, 15]}
        ti.xcom_push(key="return_value", value=extracted_data_dict)

    @task.python
    def transform_task_api(**kwargs):
        ti = kwargs["ti"]
        extracted_data_dict = ti.xcom_pull(task_ids="extract_task", key="return_value")
        api_extracted_data = extracted_data_dict["api_extracted_data"]
        print(f"Transforming data...: {api_extracted_data}...")
        transformed_api_data = [x * 10 for x in api_extracted_data]
        transformed_api_data_dict = {"transformed_api_data": transformed_api_data}
        ti.xcom_push(key="return_value", value=transformed_api_data_dict)

    @task.python
    def transform_task_db(**kwargs):
        ti = kwargs["ti"]
        db_extracted_data = ti.xcom_pull(task_ids="extract_task", key="return_value")["db_extracted_data"]
        print(f"Transforming data...: {db_extracted_data}...")
        transformed_db_data = [x * 100 for x in db_extracted_data]
        transformed_db_data_dict = {"transformed_db_data": transformed_db_data}
        ti.xcom_push(key="return_value", value=transformed_db_data_dict)

    @task.python
    def transform_task_s3(**kwargs):
        ti = kwargs["ti"]
        s3_extracted_data = ti.xcom_pull(task_ids="extract_task", key="return_value")["s3_extracted_data"]
        print(f"Transforming data...: {s3_extracted_data}...")
        transformed_s3_data = [x * 1000 for x in s3_extracted_data]
        transformed_s3_data_dict = {"transformed_s3_data": transformed_s3_data}
        ti.xcom_push(key="return_value", value=transformed_s3_data_dict)

    @task.bash
    def load_task(**kwargs):
        ti = kwargs["ti"]
        api_data = ti.xcom_pull(task_ids="transform_task_api", key="return_value")["transformed_api_data"]
        db_data = ti.xcom_pull(task_ids="transform_task_db", key="return_value")["transformed_db_data"]
        s3_data = ti.xcom_pull(task_ids="transform_task_s3", key="return_value")["transformed_s3_data"]
        return f"echo 'Loaded API data: {api_data}, DB data: {db_data}, S3 data: {s3_data}'"

    # Defining task dependencies

    extract = extract_task()
    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_s3 = transform_task_s3()
    load = load_task()

    extract >> [transform_api, transform_db, transform_s3] >> load


# Running the DAG
parallel_dags()
