from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup


with DAG("example_task_group_kv",
          start_date=datetime(2023,1,1),
          schedule_interval="@daily",
          catchup=False
         ) as dag:

    start_Task = DummyOperator(task_id = "Start_Task")
    Slack_Start_Notification = DummyOperator(task_id="Slack_Start_Notification")
    ETL_Start_Task = DummyOperator(task_id="ETL_Start_Task")

    REGIONS_to_s3_raw = DummyOperator(task_id = "REGIONS_to_s3_raw")
    REGIONS_s3_raw_to_s3_cleansed = DummyOperator(task_id="REGIONS_s3_raw_to_s3_cleansed")
    REGIONS_s3_cleansed_to_s3_publish = DummyOperator(task_id="REGIONS_s3_cleansed_to_s3_publish")


    with TaskGroup("REGIONS_Write_to_PMP", tooltip = "Tasks for Region") as  REGIONS_Write_to_PMP:
        task_1 = DummyOperator(task_id="REGIONS_s3_publish_to_dynamoDdb")
        task_2 = DummyOperator(task_id="REGIONS_s3_publish_to_opensearch")

    LOCATIONS_to_s3_raw = DummyOperator(task_id = "LOCATIONS_to_s3_raw")
    LOCATIONS_s3_raw_to_s3_cleansed = DummyOperator(task_id="LOCATIONS_s3_raw_to_s3_cleansed")
    LOCATIONS_s3_cleansed_to_s3_publish = DummyOperator(task_id="LOCATIONS_s3_cleansed_to_s3_publish")

    with TaskGroup("LOCATIONS_Write_to_PMP", tooltip = "Tasks for Location") as  LOCATIONS_Write_to_PMP:
        task_1 = DummyOperator(task_id="LOCATIONS_s3_publish_to_dynamoDdb")
        task_2 = DummyOperator(task_id="LOCATIONS_s3_publish_to_opensearch")

    JOBS_to_s3_raw = DummyOperator(task_id = "JOBS_to_s3_raw")
    JOBS_s3_raw_to_s3_cleansed = DummyOperator(task_id="JOBS_s3_raw_to_s3_cleansed")
    JOBS_s3_cleansed_to_s3_publish = DummyOperator(task_id="JOBS_s3_cleansed_to_s3_publish")

    with TaskGroup("JOBS_Write_to_PMP", tooltip = "Tasks for Jobs") as JOBS_Write_to_PMP:
        task_1 = DummyOperator(task_id="JOBS_s3_publish_to_dynamoDdb")
        task_2 = DummyOperator(task_id="JOBS_s3_publish_to_opensearch")

    COUNTRIES_to_s3_raw = DummyOperator(task_id = "COUNTRIES_to_s3_raw")
    COUNTRIES_s3_raw_to_s3_cleansed = DummyOperator(task_id="COUNTRIES_s3_raw_to_s3_cleansed")
    COUNTRIES_s3_cleansed_to_s3_publish = DummyOperator(task_id="COUNTRIES_s3_cleansed_to_s3_publish")


    with TaskGroup("COUNTRIES_Write_to_PMP", tooltip = "Tasks for Countries") as COUNTRIES_Write_to_PMP:
        task_1 = DummyOperator(task_id="COUNTRIES_s3_publish_to_dynamoDdb")
        task_2 = DummyOperator(task_id="COUNTRIES_s3_publish_to_opensearch")

    DEPARTMENTS_to_s3_raw = DummyOperator(task_id = "DEPARTMENTS_to_s3_raw")
    DEPARTMENTS_s3_raw_to_s3_cleansed = DummyOperator(task_id="DEPARTMENTS_s3_raw_to_s3_cleansed")
    DEPARTMENTS_s3_cleansed_to_s3_publish = DummyOperator(task_id="DEPARTMENTS_s3_cleansed_to_s3_publish")


    with TaskGroup("DEPARTMENTS_Write_to_PMP", tooltip = "Tasks for Department") as DEPARTMENTS_Write_to_PMP:
        task_1 = DummyOperator(task_id="DEPARTMENTS_s3_publish_to_dynamoDdb")
        task_2 = DummyOperator(task_id="DEPARTMENTS_s3_publish_to_opensearch")

    EMPLOYEEs_to_s3_raw = DummyOperator(task_id = "EMPLOYEEs_to_s3_raw")
    EMPLOYEEs_s3_raw_to_s3_cleansed = DummyOperator(task_id="EMPLOYEEs_s3_raw_to_s3_cleansed")
    EMPLOYEEs_s3_cleansed_to_s3_publish = DummyOperator(task_id="EMPLOYEEs_s3_cleansed_to_s3_publish")


    with TaskGroup("EMPLOYEEs_Write_to_PMP", tooltip = "Tasks for Employee") as EMPLOYEEs_Write_to_PMP:
        task_1 = DummyOperator(task_id="EMPLOYEEs_s3_publish_to_dynamoDdb")
        task_2 = DummyOperator(task_id="EMPLOYEEs_s3_publish_to_opensearch")

    ETL_End_Task = DummyOperator(task_id="ETL_End_Task")
    Data_Validation = DummyOperator(task_id="Data_Validation")
    Publish_Start_Task = DummyOperator(task_id="Publish_Start_Task")


    with TaskGroup("Publish_to_NSP", tooltip="Tasks for NSP Publish") as Publish_to_NSP:
        task_1 = DummyOperator(task_id="Countries_s3_publish_to_NSP")
        task_2 = DummyOperator(task_id="Departments_s3_publish_to_NSP")
        task_3 = DummyOperator(task_id="Locations_s3_publish_to_NSP")
        task_4 = DummyOperator(task_id="Jobs_s3_publish_to_NSP")
        task_5 = DummyOperator(task_id="Employees_s3_publish_to_NSP")
        task_6 = DummyOperator(task_id="Regions_s3_publish_to_NSP")

    Publish_End_task = DummyOperator(task_id = "Publish_End_task")
    Email_Success_notification = DummyOperator(task_id="Email_Success_notification")
    Slack_Success_Notification = DummyOperator(task_id="Slack_Success_Notification")
    end_Task = DummyOperator(task_id="end_Task")


start_Task >> Slack_Start_Notification >> ETL_Start_Task

ETL_Start_Task >> REGIONS_to_s3_raw >> REGIONS_s3_raw_to_s3_cleansed >> REGIONS_s3_cleansed_to_s3_publish >> REGIONS_Write_to_PMP  >> ETL_End_Task
ETL_Start_Task >> LOCATIONS_to_s3_raw >> LOCATIONS_s3_raw_to_s3_cleansed >> LOCATIONS_s3_cleansed_to_s3_publish >>  LOCATIONS_Write_to_PMP >> ETL_End_Task
ETL_Start_Task >> JOBS_to_s3_raw >> JOBS_s3_raw_to_s3_cleansed >> JOBS_s3_cleansed_to_s3_publish >> JOBS_Write_to_PMP >> ETL_End_Task
ETL_Start_Task >> COUNTRIES_to_s3_raw >> COUNTRIES_s3_raw_to_s3_cleansed >> COUNTRIES_s3_cleansed_to_s3_publish >> COUNTRIES_Write_to_PMP >> ETL_End_Task
ETL_Start_Task >> DEPARTMENTS_to_s3_raw >> DEPARTMENTS_s3_raw_to_s3_cleansed >> DEPARTMENTS_s3_cleansed_to_s3_publish >> DEPARTMENTS_Write_to_PMP >> ETL_End_Task
ETL_Start_Task >> EMPLOYEEs_to_s3_raw >> EMPLOYEEs_s3_raw_to_s3_cleansed >> EMPLOYEEs_s3_cleansed_to_s3_publish >> EMPLOYEEs_Write_to_PMP >> ETL_End_Task

ETL_End_Task >> Data_Validation >> Publish_Start_Task >> Publish_to_NSP >> Publish_End_task
Publish_End_task >> Email_Success_notification >> end_Task
Publish_End_task >> Slack_Success_Notification >> end_Task
