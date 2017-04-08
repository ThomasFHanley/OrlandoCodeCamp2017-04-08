-- Partition elimination

-- *************************************************************
USE PartitionThis 
GO

-- *********************************************************
RAISERROR('Review where we are with OrdersDaily', 0, 1) WITH NOWAIT
-- *********************************************************
SELECT *
FROM ph.vwObjectDetail
WHERE object_name IN ('OrdersDaily')
ORDER BY object_name, partition_number


RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *******************************
RAISERROR('Clean Buffers & Cache', 0, 1) WITH NOWAIT
-- *******************************
DBCC DROPCLEANBUFFERS;
DBCC FREEPROCCACHE;
-- *******************************

-- *******************************
RAISERROR('Querying our partitioned table', 0, 1) WITH NOWAIT
RAISERROR('Run looking at the Execution Plan to see the # of partitions interrogated (Actual Partition Count)', 0, 1) WITH NOWAIT
-- *******************************

SET STATISTICS IO ON
SET STATISTICS TIME ON

SELECT		COUNT(*)
FROM		dbo.OrdersDaily
WHERE		OrderDate > '2017-04-07 00:00:00.000'
			AND OrderDate < '2017-04-08 00:00:00.000'
			--AND OrderId % 2 = 0	-- Look for even Order IDs

SET STATISTICS IO OFF
SET STATISTICS TIME OFF


RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *******************************
RAISERROR('Clean Buffers & Cache', 0, 1) WITH NOWAIT
-- *******************************
DBCC DROPCLEANBUFFERS;
DBCC FREEPROCCACHE;
-- *******************************

-- *******************************
RAISERROR('Querying our partitioned table - DATA TYPES MATTER! / the BASICS still apply', 0, 1) WITH NOWAIT
RAISERROR('Run looking at the Execution Plan to see the # of partitions interrogated (Actual Partition Count)', 0, 1) WITH NOWAIT
-- *******************************

SET STATISTICS IO ON
SET STATISTICS TIME ON

SELECT		COUNT(*)
FROM		dbo.OrdersDaily
WHERE		OrderDate > CAST('2017-04-07' AS DATETIME2(0))
			AND OrderDate < CAST('2017-04-08' AS DATETIME2(0))
			--AND OrderId % 2 = 0
			
--AND OrderId BETWEEN 1500000 AND 2500000
-- DATEADD(dd,-2,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))

SET STATISTICS IO OFF
SET STATISTICS TIME OFF


RAISERROR('Breakpoint!',20,1) WITH LOG;


-- Partitioning is not entirely without consequence
-- Look at the CPU time - there is some CPU overhead to partition elimination/partitioning
-- This is a SMALL table... so the value in partition elimination is harder to see












-- ***************************
-- Resources
-- ***************************
-- Look at cached execution plans
SELECT 
  eqs.execution_count,
  CAST((1.)*eqs.total_worker_time/eqs.execution_count AS NUMERIC(10,1)) AS avg_worker_time,
  eqs.last_worker_time,
  CAST((1.)*eqs.total_logical_reads/eqs.execution_count AS NUMERIC(10,1)) AS avg_logical_reads,
  eqs.last_logical_reads,
    (SELECT TOP 1 SUBSTRING(est.text,statement_start_offset / 2+1 , 
    ((CASE WHEN statement_end_offset = -1 
      THEN (LEN(CONVERT(nvarchar(max),est.text)) * 2) 
      ELSE statement_end_offset END)  
      - statement_start_offset) / 2+1))  
    AS sql_statement,
  qp.query_plan
FROM sys.dm_exec_query_stats AS eqs
CROSS APPLY sys.dm_exec_sql_text (eqs.sql_handle) AS est 
JOIN sys.dm_exec_cached_plans cp on 
  eqs.plan_handle=cp.plan_handle
CROSS APPLY sys.dm_exec_query_plan (cp.plan_handle) AS qp
WHERE est.text like '%OrdersDaily%'
OPTION (RECOMPILE);
GO










