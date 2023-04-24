
from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator

def _choose_env(ti):
    envis = []
    if Variable.get("environment") == 'dev':
        envis.append(Variable.get("environment"))
        print(f"yes Deploy_and_Release_dev {Variable.get('environment')}")
        print(f"envis is : {envis}")
        return 'Validate_dev_Branch'
    elif Variable.get("environment") == 'qa':
        return 'Validate_qa_Branch'
    else:
        return 'Validate_prod_Branch'

with DAG("Airflow_Branching_kv", start_date=datetime(2023, 1, 1), schedule_interval = "@daily", catchup = False) as dag:

    Start_Task = DummyOperator(task_id="Start_Task")

    Validate_dev_Branch = ShortCircuitOperator(
        task_id="Validate_dev_Branch",
        python_callable=_choose_env,
        dag=dag)
    Deploy_and_Release_dev = DummyOperator(task_id="Deploy_and_Release_dev")

    Validate_qa_Branch = ShortCircuitOperator(
        task_id="Validate_qa_Branch",
        python_callable=_choose_env,
        dag=dag)
    Deploy_and_Release_qa = DummyOperator(task_id="Deploy_and_Release_qa")

    Validate_prod_Branch = ShortCircuitOperator(
        task_id="Validate_prod_Branch",
        python_callable=_choose_env,
        dag=dag)
    Deploy_and_Release_prod = DummyOperator(task_id="Deploy_and_Release_prod")

    choose_env = BranchPythonOperator(task_id="choose_env",python_callable=_choose_env )
    join = DummyOperator(task_id="join",trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,)
    End_Task = DummyOperator(task_id="End_Task")

Start_Task >> choose_env
choose_env >> Validate_dev_Branch >> Deploy_and_Release_dev >> join >> End_Task
choose_env >> Validate_qa_Branch >> Deploy_and_Release_qa >> join >> End_Task
choose_env >> Validate_prod_Branch >> Deploy_and_Release_prod >> join >> End_Task

