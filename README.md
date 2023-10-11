### A support code for final project for "Data Engineer" course by RTK
Scripts developed to imitate & analyze behaviour of iptv-like-service users

*proj-prod.py* - generate random data imitating using of service and send it to Kafka

*proj-consXX.py* - get data from Kafka and store it to HDFS

*proj-hive-XXXXX.py* - prepare data to move up using Hive

*vasilev-av-iptv-gen.py* - airflow dag launching data generation

*vasilev-av-iptv-load.py* - airflow dag launching data preparation and loading to Greenplum

*XXXXX.sql* - all DDL & DML statments used in process
