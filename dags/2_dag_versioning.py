from airflow.sdk import dag, task


@dag(dag_id="versioned_dag")
def versioned_dag():
    @task.python
    def first_task():
        print("This is the first task!")

    @task.python
    def second_task():
        print("This is the second task!")

    @task.python
    def third_task():
        print("This is the third task!")

    @task.python
    def versioned_task():
        print("This is a versioned task 2!")

    # Defining task dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    versioned = versioned_task()

    first >> second >> third >> versioned


# Running the DAG
versioned_dag()
