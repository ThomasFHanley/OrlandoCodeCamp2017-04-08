-- ***************************************************************************************************************************************************************************
-- ***************************************************************************************************************************************************************************
RAISERROR('Lock Escalation Session A', 0, 1) WITH NOWAIT
RAISERROR('This window is force locks on a partitioned and non-partitioned table and to see the resulting impact', 0, 1) WITH NOWAIT
-- ***************************************************************************************************************************************************************************

-- *************************************************************
USE PartitionThis 
GO

-- ******************************************************************************************************************
RAISERROR('Insure we are querying partition #4', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
 
SET STATISTICS IO ON;
GO

SELECT
   OBJECT_NAME(object_id) AS [Table Name],
   partition_number,
   SUM(rows) AS [SQL RowCount]
 FROM sys.partitions
 WHERE
   index_id IN (0, 1) -- 0:Heap, 1:Clustered
   AND object_id = OBJECT_ID('OrdersDaily')
 GROUP BY
   object_id, partition_number

-- First select on partition 4 range to check if data exists
SELECT		COUNT(*)
FROM		[dbo].[OrdersDaily] DLY
WHERE		OrderDate > DATEADD(dd,0,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))
			AND OrderDate < DATEADD(dd,1,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))
 


 RAISERROR('Breakpoint!',20,1) WITH LOG;


 -- ******************************************************************************************************************
RAISERROR('Force LOCK ESCALATION to the table level', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
ALTER TABLE [dbo].[OrdersDaily] SET (LOCK_ESCALATION = TABLE); --default


 RAISERROR('Breakpoint!',20,1) WITH LOG;


 -- ******************************************************************************************************************
RAISERROR('Force lock escalation on our PARTITIONED TABLE by updating all 50000 rows on 4th partition in a single, uncommitted transaction', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
BEGIN TRAN
UPDATE		DLY
SET			OrderName = OrderName + ''
FROM		[dbo].[OrdersDaily] DLY
WHERE		OrderDate > DATEADD(dd,0,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))
			AND OrderDate < DATEADD(dd,1,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))

-- *********************************************************
RAISERROR('.....', 0, 1) WITH NOWAIT
RAISERROR('Pull up Session B to see if we can count values from another partition on the same table', 0, 1) WITH NOWAIT
RAISERROR('.....', 0, 1) WITH NOWAIT
-- *********************************************************

RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *********************************************************
RAISERROR('ROLLBACK the transaction in the first session', 0, 1) WITH NOWAIT
-- *********************************************************
ROLLBACK TRAN;
GO

-- *********************************************************
RAISERROR('.....', 0, 1) WITH NOWAIT
RAISERROR('Pull up Session B to see if we got a count from the table', 0, 1) WITH NOWAIT
RAISERROR('.....', 0, 1) WITH NOWAIT
-- *********************************************************

RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *********************************************************
RAISERROR('Does the same thing happen with our NON-Partitioned table?!?', 0, 1) WITH NOWAIT
-- *********************************************************
BEGIN TRAN
UPDATE		DLY
SET			OrderName = OrderName + ''
FROM		dbo.OrdersDailyNPConsumption DLY
WHERE		OrderDate > DATEADD(dd,0,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))
			AND OrderDate < DATEADD(dd,1,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))


-- *********************************************************
RAISERROR('.....', 0, 1) WITH NOWAIT
RAISERROR('Pull up Session B to see if we can count values from another partition on the same table', 0, 1) WITH NOWAIT
RAISERROR('.....', 0, 1) WITH NOWAIT
-- *********************************************************


RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *********************************************************
RAISERROR('ROLLBACK the transaction in the first session', 0, 1) WITH NOWAIT
-- *********************************************************
ROLLBACK TRAN;
GO


RAISERROR('Breakpoint!',20,1) WITH LOG;
GO



 -- ******************************************************************************************************************
RAISERROR('On our PARITITIONED TABLE, Enable LOCK ESCALATION = AUTO to enable/allow partition level lock ', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
ALTER TABLE [dbo].[OrdersDaily] SET (LOCK_ESCALATION = AUTO);

BEGIN TRAN
UPDATE		DLY
SET			OrderName = OrderName + ''
FROM		[dbo].[OrdersDaily] DLY
WHERE		OrderDate > DATEADD(dd,0,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))
			AND OrderDate < DATEADD(dd,1,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))


RAISERROR('Breakpoint!',20,1) WITH LOG;
GO


-- *********************************************************
RAISERROR('ROLLBACK the transaction in the first session', 0, 1) WITH NOWAIT
-- *********************************************************
ROLLBACK TRAN;
GO
