drop external table if exists vasilev_av_iptv_05_10_23;
create external table vasilev_av_iptv_05_10_23
       (id int8, start_time timestamp, end_time timestamp, content_id integer, content_name text, release_date date, content_type_id integer, content_genre_id integer, user_id integer, user_name text, birth_date date, gender int2)
location ('pxf://vasilev_av.iptv_all_05_10_23?PROFILE=hive&SERVER=hadoop') format 'custom' (FORMATTER='pxfwritable_import');

drop table if exists vasilev_av_iptv_current cascade;
create table vasilev_av_iptv_current with (appendoptimized=true, orientation=column)
as select id, start_time, div(extract(epoch from end_time-start_time)::integer,60) as watch_minutes,
          content_id, content_name, release_date, content_type_id, content_genre_id,
          user_id, user_name, div(current_date-birth_date,365) as user_age, gender
from vasilev_av_iptv_05_10_23 distributed randomly;

create or replace view vasilev_av_iptv_current_groups
as select vaic.*, date(vaic.start_time) as whatch_date,
          extract (hour from vaic.start_time) as watch_hour,
          date_trunc('hour', vaic.start_time) as watch_date_hour,
          case when user_age<20 then '20-30'
               when user_age between 20 and 30 then '20-30'
               when user_age between 30 and 40 then '30-40'
               when user_age between 40 and 50 then '40-50'
               when user_age between 50 and 60 then '50-60'
               else '>60' end as age_group,
          case when gender=0 then 'M'
               when gender=1 then 'F' end as user_gender
from vasilev_av_iptv_current vaic;
