SELECT  region, datasource, count(*) as count FROM jobsity.table_trips
where month(table_trips.datetime) < 6
group by region, datasource
order by table_trips.datetime DESC
limit 2