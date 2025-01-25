## 1. 24.3.1
## 2. db:5432


## 3. 104,838; 199,013; 109,645; 27,688; 35,202

```sql
SELECT 
    CASE
        WHEN "trip_distance" <= 1 THEN 'Up to 1 mile'
        WHEN "trip_distance" > 1 AND "trip_distance" <= 3 THEN 'Between 1 and 3 miles'
        WHEN "trip_distance" > 3 AND "trip_distance" <= 7 THEN 'Between 3 and 7 miles'
        WHEN "trip_distance" > 7 AND "trip_distance" <= 10 THEN 'Between 7 and 10 miles'
        WHEN "trip_distance" > 10 THEN 'Over 10 miles'
    END AS "trip_segment",
    COUNT(*) AS "trip_count"
FROM public."green_taxi_data"
WHERE "lpep_pickup_datetime" >= '2019-10-01' 
  AND "lpep_pickup_datetime" < '2019-11-01'
GROUP BY "trip_segment"
ORDER BY "trip_segment";
```




## 4. 2019-10-31

```sql
SELECT 
    DATE("lpep_pickup_datetime") AS "pickup_day",
    MAX("trip_distance") AS "longest_trip_distance"
FROM public."green_taxi_data"
WHERE "lpep_pickup_datetime" >= '2019-10-01' 
  AND "lpep_pickup_datetime" < '2019-11-01'
GROUP BY "pickup_day"
ORDER BY "pickup_day";
```
## 5.East Harlem North, East Harlem South, Morningside Heights

```sql
SELECT 
    z."Zone" AS "pickup_zone",
    SUM(gtd."total_amount") AS "total_revenue"
FROM public."green_taxi_data" gtd
JOIN public."zones" z ON gtd."PULocationID" = z."LocationID"
WHERE DATE(gtd."lpep_pickup_datetime") = '2019-10-18'
GROUP BY z."Zone"
HAVING SUM(gtd."total_amount") > 13000
ORDER BY "total_revenue" DESC
LIMIT 3;
```


## 6.JFK Airport

```sql
select *
FROM public.green_taxi_data t
join public.zones z on t."PULocationID" =  z."LocationID"
order by t."tip_amount" DESC
```

## 7. terraform init, terraform apply -auto-approve, terraform destroy