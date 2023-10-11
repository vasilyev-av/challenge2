from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession, Row
from datetime import date


spark = SparkSession.builder.getOrCreate()

consumer = KafkaConsumer('vasilev_av_iptv_acc', group_id='vasilev_av_group3', bootstrap_servers='vm-strmng-s-1.test.local:9092')

counter=1
print(f'start to consume {counter}')
try:
    lines=list()
    for msg in consumer:
        line=json.loads(msg.value.decode('utf-8'))
        #print(line)
        print(counter)
        lines.append(list(line.values()))

        if counter==10000 :
            today=date.today().strftime('%d_%m_%y')
            parquet_name=f'hdfs://vm-dlake2-m-1.test.local/user/vasilev-av/data/acc_{today}.parquet'
            df = spark.createDataFrame(lines, list(line.keys()))
            #df.show()
            df.write.save(parquet_name, format='parquet', mode='overwrite')
            counter=1
            print("wrote a file")

        counter+=1

finally:
    consumer.close()
    spark.stop()

