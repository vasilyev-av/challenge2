from pyhive import hive
import datetime

today=datetime.date.today().strftime('%d_%m_%y')

HIVE_INTERNAL_DROP = f'drop table if exists iptv_all_{today}'

with hive.connect(host='vm-dlake2-m-1.test.local', port=10000, database='vasilev_av'
) as hql:
    cursor = hql.cursor()
    print('drop union table')
    cursor.execute(HIVE_INTERNAL_DROP)
    print('done')

