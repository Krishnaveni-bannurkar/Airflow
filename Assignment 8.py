from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import ShortCircuitOperator
import json
from datetime import datetime
from itertools import groupby

countryPopulation = {}

with open('dags/countries.json') as datafile:
    data = json.load(datafile)
newData = data['countries']['country']

# key_func to specify that we want to sort by a particular key in the dictionary
def key_func(k):
    return k['continentName']

#sort tha data based on the key_func(continentName)
newData = sorted(newData, key=key_func)

#The groupby() function returns an iterator of pairs, where the first element of each pair is the key (the continentname),
#  and the second element is an iterator that produces the elements in the group.
#The for loop will iterate over these pairs, one at a time, assigning the continent name to key and the corresponding group iterator to value.
for key, value in groupby(newData, key_func):
    totalPopulation = 0
    key = key.replace(" ", "_")
    for val in list(value):
        numVal = int(val['population'])
        totalPopulation += numVal
        # print(totalPopulation)
    countryPopulation[key] = totalPopulation


def readData():
    print(countryPopulation)
    return countryPopulation

#pushes the data variable to XCom with the key 'countryPopulation'
def push_population(**context):
    data = readData()
    context["ti"].xcom_push(key='countryPopulation', value=data)
    return data


def pull_population(t, **context):
    x = context["ti"].xcom_pull(
        key='countryPopulation', task_ids=['Process_Data'])
    print(f"Population count of {t} is {x[0][t]}")
    return x[0][t]

with DAG(
    dag_id='example_task_group_kv1',
    start_date=datetime(2023,1,1),
    schedule_interval="@Daily",
    catchup=False,
    tags=['example']


)as dag:

    Start_Task = DummyOperator(
        task_id='Start_Task',
        dag=dag,
    )
    Process_Data = ShortCircuitOperator(
        task_id="Process_Data",
        python_callable=push_population,
        provide_context=True,
        dag=dag,

    )
    End_Task = DummyOperator(
        task_id='End_Task',
        dag=dag,
        # trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    Start_Task >> Process_Data
    for t, val in countryPopulation.items():
        x = ShortCircuitOperator(
            task_id=f"Population_Of_{t}",
            provide_context=True,
            python_callable=pull_population,
            op_args=[t]

        )
        Process_Data >> x >> End_Task