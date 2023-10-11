import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.ssh.operators.ssh import SSHOperator


today=datetime.date.today().strftime('%d_%m_%y')

DAG_NAME='vasilev-av-iptv_load'
GP_CONN_ID = 'vasilevav'
GP_EXTERNAL_CREATE = f"create external table vasilev_av_iptv_{today} (id int8, start_time timestamp, end_time timestamp, content_id integer, content_name text, release_date date, content_type_id integer, content_genre_id integer, user_id integer, user_name text, birth_date date, gender int2) location ('pxf://vasilev_av.iptv_all_{today}?PROFILE=hive&SERVER=hadoop') format 'custom' (FORMATTER='pxfwritable_import');"
GP_INTERNAL_DROP = f'drop table if exists vasilev_av_iptv_current cascade;'
GP_INTERNAL_CREATE = f"create table vasilev_av_iptv_current with (appendoptimized=true, orientation=column) as select id, start_time, div(extract(epoch from end_time-start_time)::integer,60) as watch_minutes, content_id, content_name, release_date, content_type_id, content_genre_id, user_id, user_name, div(current_date-birth_date,365) as user_age, gender from vasilev_av_iptv_{today} distributed randomly;"
GP_EXTERNAL_DROP = f'drop external table if exists vasilev_av_iptv_{today};'
GP_VIEW_CREATE = f"create or replace view vasilev_av_iptv_current_groups as select vaic.*, date(vaic.start_time) as whatch_date, extract (hour from vaic.start_time) as watch_hour, date_trunc('hour', vaic.start_time) as watch_date_hour, case when user_age<20 then '20-30' when user_age between 20 and 30 then '20-30' when user_age between 30 and 40 then '30-40' when user_age between 40 and 50 then '40-50' when user_age between 50 and 60 then '50-60' else '>60' end as age_group, case when gender=0 then 'M' when gender=1 then 'F' end as user_gender from vasilev_av_iptv_current vaic;"

default_args={'owner': 'vasilevav', 'start_date': datetime.datetime.now()}

with DAG(
    DAG_NAME,
    description='load data into gp',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    max_active_runs=1,
    params={'labels':{'env': 'prod', 'priority': 'high'}}
    ):

    make_hive_source = SSHOperator(task_id = 'create_hive_tables',
         ssh_conn_id = 'vasilev-av_ssh_vm-cli2_conn',
         command = 'source python/venv/bin/activate; python3 python/proj-hive-create.py')

    make_gp_external=PostgresOperator(
        task_id='gp_external_create',
        postgres_conn_id=GP_CONN_ID,
        sql=GP_EXTERNAL_CREATE,
        autocommit=True)

    drop_gp_internal=PostgresOperator(
        task_id='gp_internal_drop',
        postgres_conn_id=GP_CONN_ID,
        sql=GP_INTERNAL_DROP,
        autocommit=True)

    make_gp_internal=PostgresOperator(
        task_id='gp_internal_create',
        postgres_conn_id=GP_CONN_ID,
        sql=GP_INTERNAL_CREATE,
        autocommit=True)

    drop_gp_external=PostgresOperator(
        task_id='gp_external_drop',
        postgres_conn_id=GP_CONN_ID,
        sql=GP_EXTERNAL_DROP,
        autocommit=True)

    drop_hive_source = SSHOperator(task_id = 'clear_hive_tables',
         ssh_conn_id = 'vasilev-av_ssh_vm-cli2_conn',
         command = 'source python/venv/bin/activate; python3 python/proj-hive-clear.py')

    make_gp_view=PostgresOperator(
        task_id='gp_view_create',
        postgres_conn_id=GP_CONN_ID,
        sql=GP_VIEW_CREATE,
        autocommit=True)

    make_hive_source >> [make_gp_external, drop_gp_internal]
    make_gp_external >> [make_gp_internal, drop_gp_external]
    make_gp_internal >> [make_gp_view, drop_hive_source]


