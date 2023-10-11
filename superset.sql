
--content type age group
select content_type_id,
age_group,
count(watch_minutes) as watch_count,
sum(watch_minutes) as watch_sum
from vasilev_av_iptv_current_groups vaig
group by content_type_id, age_group
order by content_type_id, age_group
;

--content genre age group
select content_genre_id,
age_group,
count(watch_minutes) as watch_count,
sum(watch_minutes) as watch_sum
from vasilev_av_iptv_current_groups vaig
group by content_genre_id, age_group
order by content_genre_id, age_group
;

--content type gender
select content_type_id,
user_gender,
count(watch_minutes) as watch_count,
sum(watch_minutes) as watch_sum
from vasilev_av_iptv_current_groups vaig
group by content_type_id, user_gender
order by content_type_id, user_gender
;

--content genre gender
select content_genre_id,
user_gender,
count(watch_minutes) as watch_count,
sum(watch_minutes) as watch_sum
from vasilev_av_iptv_current_groups vaig
group by content_genre_id, user_gender
order by content_genre_id, user_gender
;

--watch hour gender
select watch_hour,
user_gender,
count(watch_minutes) as watch_count,
sum(watch_minutes) as watch_sum
from vasilev_av_iptv_current_groups vaig
group by watch_hour, user_gender
order by watch_hour, user_gender
;

--watch hour age group
select watch_hour,
age_group,
count(watch_minutes) as watch_count,
sum(watch_minutes) as watch_sum
from vasilev_av_iptv_current_groups vaig
group by watch_hour, age_group
order by watch_hour, age_group 
;

--watch date hour content type
select watch_date_hour,
content_type_id,
count(watch_minutes) as watch_count,
sum(watch_minutes) as watch_sum
from vasilev_av_iptv_current_groups vaig
group by watch_date_hour, content_type_id
order by watch_date_hour, content_type_id
;

--watch date hour content genre
select watch_date_hour,
content_genre_id,
count(watch_minutes) as watch_count,
sum(watch_minutes) as watch_sum
from vasilev_av_iptv_current_groups vaig
group by watch_date_hour, content_genre_id
order by watch_date_hour, content_genre_id
;

--watch date hour total
select watch_date_hour,
count(watch_minutes) as watch_count,
sum(watch_minutes) as watch_sum
from vasilev_av_iptv_current_groups vaig
group by watch_date_hour
order by watch_date_hour
;

select
content_name,
count(watch_minutes) as watch_count,
sum(watch_minutes) as watch_sum
from vasilev_av_iptv_current_groups vaig
group by content_id, content_name
order by sum(watch_minutes) desc, content_name
limit 10;