from pyhive import hive
import datetime

today=datetime.date.today().strftime('%d_%m_%y')

HIVE_EXTERNAL1_CREATE = f'create external table if not exists iptv_user_{today} (id string, name string, birth_date string, gender string) stored as parquet location "/user/vasilev-av/data/usr_{today}.parquet/"'
HIVE_EXTERNAL2_CREATE = f'create external table if not exists iptv_content_{today} (id string, name string, release_date string, type string, genre string) stored as parquet location "/user/vasilev-av/data/cnt_{today}.parquet/"'
HIVE_EXTERNAL3_CREATE = f'create external table if not exists iptv_access_{today} (uset_id string, content_id string, start_time string, end_time string, id string) stored as parquet location "/user/vasilev-av/data/acc_{today}.parquet/"'
HIVE_INTERNAL_CREATE = f'create table if not exists iptv_all_{today} as select cast(ia.id as bigint) as id, cast(ia.start_time as timestamp) as start_time, cast(ia.end_time as timestamp) as end_time, cast(ic.id as int) as content_id, ic.name as content_name, cast(ic.release_date as date) as release_date, cast(ic.type as int) as content_type_id, cast(ic.genre as int) as content_genre_id, cast(iu.id as int) as user_id, iu.name as user_name, cast(iu.birth_date as date) as birth_date, cast(iu.gender as tinyint) as gender from iptv_access_{today} ia join iptv_content_{today} ic on (ia.content_id=ic.id) join iptv_user_{today} iu on (ia.uset_id=iu.id)'
HIVE_INTERNAL_DROP = f'drop table if exists iptv_all_{today}'

with hive.connect(host='vm-dlake2-m-1.test.local', port=10000, database='vasilev_av'
) as hql:
    cursor = hql.cursor()
    print('create user source table')
    cursor.execute(HIVE_EXTERNAL1_CREATE)
    print('create content source table')
    cursor.execute(HIVE_EXTERNAL2_CREATE)
    print('create access source table')
    cursor.execute(HIVE_EXTERNAL3_CREATE)
    print('create union table')
    cursor.execute(HIVE_INTERNAL_CREATE)
    print('done')


