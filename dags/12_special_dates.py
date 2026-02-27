from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable

special_dates = EventsTimetable(
    event_dates=[
        datetime(year=2026, month=3, day=26, tz="Europe/Berlin"),
        datetime(year=2026, month=3, day=31, tz="Europe/Berlin"),
        datetime(year=2026, month=3, day=25, tz="Europe/Berlin"),
        datetime(year=2026, month=3, day=7, tz="Europe/Berlin"),
        datetime(year=2026, month=3, day=5, tz="Europe/Berlin"),
        datetime(year=2026, month=3, day=1, tz="Europe/Berlin"),
    ]
)


@dag(
    dag_id="special_dates_dag",
    schedule=special_dates,
    start_date=datetime(year=2026, month=3, day=1, tz="Europe/Berlin"),
    end_date=datetime(year=2026, month=3, day=31, tz="Europe/Berlin"),
    catchup=True,
)
def special_dates_dag():
    @task.python
    def special_event_task(**kwargs):
        execution_date = kwargs["logical_date"]
        print(f"Running special event task for {execution_date}")

    special_event = special_event_task()


# Running the DAG
special_dates_dag()
