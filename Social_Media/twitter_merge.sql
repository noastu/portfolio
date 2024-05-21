update twitter_cities
set id = z_twitter_cities.id,
    created_at = to_timestamp(z_twitter_cities.created_at, 'YYYY-MM-DD THH24:MI:SSZ'),
    text = z_twitter_cities.text,
    possibly_sensitive = z_twitter_cities.possibly_sensitive,
    search_string = z_twitter_cities.search_string,
    batch_date = z_twitter_cities.batch_date
from z_twitter_cities
where twitter_cities.id = z_twitter_cities.id;

insert into twitter_cities as tgt
select src.id  
    ,to_timestamp(src.created_at, 'YYYY-MM-DD THH24:MI:SSZ') 
    ,src.text
    ,src.possibly_sensitive
    ,src.search_string
    ,src.batch_date
from z_twitter_cities src
left outer join twitter_cities tgt on src.id = tgt.id 
where tgt.id is null;

truncate table z_twitter_cities;