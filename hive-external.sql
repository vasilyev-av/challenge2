drop table if exists iptv_user_05_10_23;
create external table if not exists iptv_user_05_10_23 (id string, name string, birth_date string, gender string) stored as parquet location "/user/vasilev-av/data/usr_05_10_23.parquet/";
drop table if exists iptv_content_05_10_23;
create external table if not exists iptv_content_05_10_23 (id string, name string, release_date string, type string, genre string) stored as parquet location "/user/vasilev-av/data/cnt_05_10_23.parquet/";
drop table if exists iptv_access_05_10_23;
create external table if not exists iptv_access_05_10_23 (uset_id string, content_id string, start_time string, end_time string, id string) stored as parquet location "/user/vasilev-av/data/acc_05_10_23.parquet/";

drop table if exists iptv_all_05_10_23;
create table if not exists iptv_all_05_10_23 as select cast(ia.id as bigint) as id, cast(ia.start_time as timestamp) as start_time, cast(ia.end_time as timestamp) as end_time, cast(ic.id as int) as content_id, ic.name as content_name, cast(ic.release_date as date) as release_date, cast(ic.type as int) as content_type_id, cast(ic.genre as int) as content_genre_id, cast(iu.id as int) as user_id, iu.name as user_name, cast(iu.birth_date as date) as birth_date, cast(iu.gender as tinyint) as gender from iptv_access_05_10_23 ia join iptv_content_05_10_23 ic on (ia.content_id=ic.id) join iptv_user_05_10_23 iu on (ia.uset_id=iu.id);
