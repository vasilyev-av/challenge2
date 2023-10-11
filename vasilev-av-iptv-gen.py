from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

DAG_NAME = 'vasilev-av-iptv-gen'

args = {'owner': 'vasilev-av',
    'start_date': datetime.now()
    }

with DAG(DAG_NAME,
    default_args = args,
    description = 'generate data',
    schedule_interval = '@once',
    catchup = False,
    params = {'labels':{'env': 'prod', 'priority': 'high'}}
    ) as dag:

    iptv_data_generator = SSHOperator(task_id = 'generate_iptv_data',
                                      ssh_conn_id = 'vasilev-av_ssh_vm-cli2_conn',
                                      command = 'python3 python/proj-prod.py')

    iptv_data_generator
