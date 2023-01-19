SELECT 
  count(*) 
FROM 
  public.green_taxi_trips 
WHERE 
  date_trunc('day', lpep_pickup_datetime) = '2019-01-15' 
  AND date_trunc('day', lpep_dropoff_datetime) = '2019-01-15';

SELECT 
  lpep_pickup_datetime 
FROM 
  public.green_taxi_trips 
WHERE 
  trip_distance =(
    SELECT 
      max(trip_distance) AS max_distance 
    FROM 
      public.green_taxi_trips 
    GROUP BY 
      trip_distance 
    ORDER BY 
      max_distance DESC 
    LIMIT 
      1
  );


SELECT 
  passenger_count, 
  count(*) 
FROM 
  public.green_taxi_trips 
WHERE 
  date_trunc('day', lpep_pickup_datetime) = '2019-01-01' 
GROUP BY 
  passenger_count 
HAVING 
  passenger_count IN (2, 3);


SELECT 
  "Zone" 
FROM 
  (
    SELECT 
      max(q.tip_amount) AS max_tip, 
      q."DOLocationID" AS locationid 
    FROM 
      (
        SELECT 
          * 
        FROM 
          public.green_taxi_trips gt 
          LEFT JOIN public.taxi_zones tz ON "PULocationID" = "LocationID" 
        WHERE 
          "Zone" = 'Astoria'
      ) q 
    GROUP BY 
      q."DOLocationID" 
    ORDER BY 
      max_tip DESC 
    LIMIT 
      1
  ) q2 
  JOIN public.taxi_zones tz ON locationid = "LocationID";