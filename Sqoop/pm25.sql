# 建立新表 字段为 dt,id,pm25,address,city
create table if not exists pm25_city(
dt string,
id string,
pm25 string,
address string,
city string
)row format delimited fields terminated by ",";

insert into table pm25_city 
select p.dt,p.id,p.pm25,c.address,c.city
from pm25 p join city_air c on p.id=c.id;


# 求出每个address 一年中平均pm2.5的值放入新表中 新表字段为id,address,city,avg_pm25
create table if not exists avg_pm(
id string,
address string,
city string,
avg_pm25 string
)row format delimited fields terminated by ",";

insert into table avg_pm 
select id,address,city,avg(pm25) avg_pm25 
from pm25_city 
group by id,address,city;