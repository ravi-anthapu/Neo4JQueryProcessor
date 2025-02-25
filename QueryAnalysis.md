# Query Log Analysis

Once the SQLite database is generated we can use these queries to understand the performance of the system.

## Total Unique Queries

We want to know how many total unique queries were executed in this period of query log capture.
This is important to know how our Query cache is impacted. If we have more unique
queries than the query cache we might spend more time in planning, which is waste of CPU.

Also, this might indicate we might not be parameterizing our queries. This is not a good practice.

``` sql
SELECT count(1) FROM queries
```

## Planning Time Percentage

We want to understand what amount of time we are spending in planning when compared to the total execution time. 
This is an important metric to understand if our queries are parameterized correctly or not. 
This value should be close to zero. More time we are spending in planning
we are not using that CPU time to execute the queries.

``` sql
SELECT  sum(planning) *100.0/  sum(elapsedTimeMs)  as planPercent from query_execution   
```

## Percentage of Queries Planned

We want to see how often the queries are being planned. A query can go through planning either due to the amount of data changed in the database or 
system cannot find a plan for a given query. 

This also should be close to zero. 

``` sql
SELECT (select count(1) from query_execution where planning > 0 ) *100.0/ (select count(1) from query_execution) as planPercent
```

## Number of transactions per server

This query returns the number of queries executed by each server. 

This is important to look in cluster environment to understand if the READ traffic is 
being directed to FOLLOWERS and SECONDARIES. If we are not using the READ/WRITE
transactions correctly with the driver, then all or most of the queries will be sent to
the LEADER only. 

````sql
SELECT server, count(1) FROM query_execution 
GROUP BY server
````

## Traffic Analysis per server per second

This query returns the traffic per server, per second. It returns number of queries 
executed per second, total time taken, total page hits, total page faults etc.

This tells us the traffic patterns and if we are seeing lot pf page hits and page faults 
together then we might have to tune the queries or increase the memory. If we are seeing lot of waiting, then it 
is possible we are cpu bound and may need more computing power. 

If we see occasional spike and traffic settles down, then we might have a peak traffic issue.
If the SLA's are acceptable then we are good with this. If not then we need to see if
we need to tune the queries or look at hardware specifications. 

````sql
SELECT server, 
       DATETIME(start_timeStamp/1000, 'unixepoch' ) as time, 
       count(1) as count, 
        sum(elapsedtimems) as totalTime, 
        sum(planning) as planning, 
        sum(cputime) as cpuTime, 
        sum(waiting) as waiting
        sum(pagehits) as pagehits, 
        sum(pagefaults) as pagefaults 
from query_execution GROUP by server, time

````

## Statistics by Query

This query returns all the queries with the number of times it is executed
and average pge hits, page faults, time taken etc.

We can identify the queries that take lot of page hits or page faults and see
what we need to concentrate on fixing. 

Along with averages, we can add min, max to the query if we want to see more information.

````sql
SELECT  q.id as id,
    query as query,
    count(1) as count,
    avg(pagehits) as pagehits,
    avg(pagefaults) as pagefaults,
    avg(elapsedtimems) as time,
    avg(cputime) as cputime,
    avg(planning) as planning,
    avg(waiting) as waiting
FROM queries q, query_execution qe
WHERE q.id = qe.query_id 
GROUP BY id
````

## Query statistics for lower 90 percentile stats vs upper 10 percentile stats

This query gives the lower 90 percentile statistics for a query and the upper 10 percentile stats 
by the time taken so that we can understand which queries have an outliers in terms of performance.

If a query has much higher values for the upper 10 percentile stats, then it is possible 
that there are certain parameters that can cause this query to take lot of time. This means
there are some data anomalies we need to understand or tune the query to handle those scenarios well.

````sql 
SELECT lower90.query_id as query_id, minTime_lower90, maxTime_lower90, avgTime_lower90, count_lower90, minTime_upper90, maxTime_upper90, avgTime_upper90, count_upper90
FROM 
( SELECT query_id, min(elapsedTimeMs) as minTime_lower90, max(elapsedTimeMs) as maxTime_lower90, avg(elapsedTimeMs) as avgTime_lower90, count(1) as count_lower90
FROM (
		SELECT query_id, elapsedTimeMs, SizePercentRank
		FROM (
			SELECT
				query_id,
				elapsedTimeMs,
				PERCENT_RANK() OVER( 
					PARTITION BY query_id
					ORDER BY elapsedTimeMs ASC
				) SizePercentRank
		FROM
		query_execution
	) WHERE SizePercentRank < 0.9
) GROUP BY query_id ) lower90
LEFT JOIN (SELECT query_id, min(elapsedTimeMs) as minTime_upper90, max(elapsedTimeMs) as maxTime_upper90, avg(elapsedTimeMs) as avgTime_upper90, count(1) as count_upper90
FROM (
		SELECT query_id, elapsedTimeMs, SizePercentRank
		FROM (
			SELECT
				query_id,
				elapsedTimeMs,
				PERCENT_RANK() OVER( 
					PARTITION BY query_id
					ORDER BY elapsedTimeMs ASC
				) SizePercentRank
		FROM
		query_execution
	) WHERE SizePercentRank >=0.9
) GROUP BY query_id ) upper90 ON lower90.query_id=upper90.query_id
````