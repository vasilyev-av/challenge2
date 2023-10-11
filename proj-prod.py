from kafka import KafkaProducer
import json
import math
import numpy as np
from faker import Faker
from datetime import date, datetime, timedelta

producer=KafkaProducer(bootstrap_servers='vm-strmng-s-1.test.local:9092')
fake=Faker()

for x in range(100):
    ur_id=str(x)
    ur_name="user name "+str(x)
    ur_dat=str(fake.date_between(date(1960, 1, 1), date(2000, 1, 1)))
    ur_sex=str(x%2)
    user={"id": ur_id, "name": ur_name, "birth_date": ur_dat, "gender": ur_sex}
    producer.send('vasilev_av_iptv_usr', json.dumps(user).encode('utf-8'))

for x in range(100):
    cont_id=str(x)
    cont_name="content name "+str(x)
    cont_dat=str(fake.date_between(date(2010, 1, 1), date(2020, 1, 1)))
    cont_type=str(x%5)
    cont_genre=str(x%10)
    content={"id": cont_id, "name": cont_name, "release_date": cont_dat, "type": cont_type, "genre": cont_genre}
    producer.send('vasilev_av_iptv_cnt', json.dumps(content).encode('utf-8'))

usr = np.random.default_rng().normal(50, 50, 10000)
usr = list(map(abs, usr))
usr = list(map(lambda x: x%100, usr))
cnt = np.random.default_rng().normal(50, 50, 10000)
cnt = list(map(abs, cnt))
cnt = list(map(lambda x: x%100, cnt))
mnt = np.random.default_rng().normal(30, 30, 10000)
mnt = list(map(abs, mnt))
#mnt = list(map(lambda x: x%120, h))

usage=list()
for x in range(10000):
    acc_user=str(int(usr[x]))
    acc_cont=str(int(cnt[x]))
    sdat=datetime(2023, 9, 1+(x%3)+(int(abs(math.sin(x/3)*10))*int(abs(math.sin(x/4)*4))),0+(int(abs(math.sin(x/4)*24))),x%60,int(mnt[x]/2)%60)
    acc_sdat=str(sdat)
    acc_edat=str(sdat+timedelta(minutes=int(mnt[x]), seconds=int(mnt[x])%60))
    access={"uset_id": acc_user, "content_id": acc_cont, "start_time": acc_sdat, "end_time": acc_edat}
    usage.append(access)

def get_start_time(e):
    return e['start_time']

usage.sort(key=get_start_time)
for x in range(10000):
    usage[x]["id"]=x
    producer.send('vasilev_av_iptv_acc', json.dumps(usage[x]).encode('utf-8'))

producer.close()
