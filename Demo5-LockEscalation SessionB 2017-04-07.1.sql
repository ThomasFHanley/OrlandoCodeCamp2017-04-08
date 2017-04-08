-- ***************************************************************************************************************************************************************************
-- ***************************************************************************************************************************************************************************
RAISERROR('Lock Escalation Session B', 0, 1) WITH NOWAIT
RAISERROR('This is where we try to query the locked object and where we look at the locks', 0, 1) WITH NOWAIT
-- ***************************************************************************************************************************************************************************

USE PartitionThis 
GO

-- *********************************************************
RAISERROR('Let''s look at the existing locks and compare them to the table', 0, 1) WITH NOWAIT
-- *********************************************************
SELECT	* 
FROM	sys.partitions 
WHERE	(	object_id = OBJECT_ID ('OrdersDaily')
			AND partition_number = 4
		)
		OR
		(	object_id = OBJECT_ID ('OrdersDailyNPConsumption')	)

GO

SELECT
t1.resource_type,
t1.resource_database_id,
t1.resource_associated_entity_id,
t1.request_mode,
t1.request_session_id,
t2.blocking_session_id,
o1.name 'object name',
o1.type_desc 'object descr',
p1.partition_id 'partition id',
p1.rows 'partition/page rows',
a1.type_desc 'index descr',
a1.container_id 'index/page container_id'
FROM sys.dm_tran_locks as t1
LEFT OUTER JOIN sys.dm_os_waiting_tasks as t2
    ON t1.lock_owner_address = t2.resource_address
LEFT OUTER JOIN sys.objects o1 on o1.object_id = t1.resource_associated_entity_id
LEFT OUTER JOIN sys.partitions p1 on p1.hobt_id = t1.resource_associated_entity_id
LEFT OUTER JOIN sys.allocation_units a1 on a1.allocation_unit_id = t1.resource_associated_entity_id


 RAISERROR('Breakpoint!',20,1) WITH LOG;


 -- *********************************************************
RAISERROR('Let''s try to query Partition #5', 0, 1) WITH NOWAIT
-- *********************************************************
USE PartitionThis ;
GO
SELECT		COUNT (*) 
FROM		[dbo].[OrdersDaily] DLY
WHERE		OrderDate > DATEADD(dd,1,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))
			AND OrderDate < DATEADD(dd,2,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))
GO


 RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *********************************************************
RAISERROR('.....', 0, 1) WITH NOWAIT
RAISERROR('Go back to Session A and rollback the transaction', 0, 1) WITH NOWAIT
RAISERROR('.....', 0, 1) WITH NOWAIT
-- *********************************************************





 RAISERROR('Breakpoint!',20,1) WITH LOG;




 -- Round 2
 -- *********************************************************
RAISERROR('Let''s try to query Partition #5', 0, 1) WITH NOWAIT
-- *********************************************************
USE PartitionThis ;
GO
SELECT		COUNT (*) 
FROM		[dbo].OrdersDailyNPConsumption DLY
WHERE		OrderDate > DATEADD(dd,1,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))
			AND OrderDate < DATEADD(dd,2,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))
GO



 -- *********************************************************
RAISERROR('Scroll up and re-run the code from before hitting partition #5', 0, 1) WITH NOWAIT
-- *********************************************************