--******************
--Copyright 2013, Brent Ozar PLF, LLC DBA Brent Ozar Unlimited.
--******************

--******************
--Don't just run the whole thing.
--Run this step by step to learn!
--******************
DECLARE @msg NVARCHAR(MAX);
SET @msg = N'Did you mean to run this whole script?' + CHAR(10)
    + N'MAKE SURE YOU ARE RUNNING AGAINST A TEST ENVIRONMENT ONLY!'

RAISERROR(@msg,20,1) WITH LOG;
GO

-- ***************************************************************************************************************************************************************************
-- ***************************************************************************************************************************************************************************
RAISERROR('Horizontal Partitioning via table partitioning', 0, 1) WITH NOWAIT
-- ***************************************************************************************************************************************************************************

-- ******************************************************************************************************************
RAISERROR('1- CREATE OUR DEMO DATABASE - Blow it away if it already exists', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************

IF db_id('PartitionThis') IS NOT NULL 
BEGIN
	USE master; 
	ALTER DATABASE [PartitionThis] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE [PartitionThis];
END 
GO

CREATE DATABASE [PartitionThis]
GO

ALTER DATABASE [PartitionThis]
	MODIFY FILE ( NAME = N'PartitionThis', SIZE = 256MB , MAXSIZE = 10GB , FILEGROWTH = 512MB );
ALTER DATABASE [PartitionThis]	
	MODIFY FILE ( NAME = N'PartitionThis_log', SIZE = 128MB , FILEGROWTH = 128MB );
GO

USE PartitionThis;
GO

-- ******************************************************************************************************************
RAISERROR('2- CREATE HELPER OBJECTS to allow us to keep track of partitioning', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************

-- *********************************************************
RAISERROR('Create a schema for "partition helper" objects', 0, 1) WITH NOWAIT
-- *********************************************************
GO
CREATE SCHEMA [ph] AUTHORIZATION dbo;
GO

-- *********************************************************
RAISERROR('Create a view to see partition information by filegroup', 0, 1) WITH NOWAIT
-- *********************************************************
IF OBJECT_ID('ph.vwFileGroupDetail', 'V') IS NOT NULL DROP VIEW ph.vwFileGroupDetail
GO
CREATE VIEW ph.vwFileGroupDetail
AS
SELECT  pf.name AS pf_name ,
        ps.name AS partition_scheme_name ,
        p.partition_number ,
        ds.name AS partition_filegroup ,
        pf.type_desc AS pf_type_desc ,
        pf.fanout AS pf_fanout ,
        pf.boundary_value_on_right ,
        OBJECT_NAME(si.object_id) AS object_name ,
        rv.value AS range_value ,
        SUM(CASE WHEN si.index_id IN ( 1, 0 ) THEN p.rows
                    ELSE 0
            END) AS num_rows ,
        SUM(dbps.reserved_page_count) * 8 / 1024. AS reserved_mb_all_indexes ,
        SUM(CASE ISNULL(si.index_id, 0)
                WHEN 0 THEN 0
                ELSE 1
            END) AS num_indexes
FROM    sys.destination_data_spaces AS dds
        JOIN sys.data_spaces AS ds ON dds.data_space_id = ds.data_space_id
        JOIN sys.partition_schemes AS ps ON dds.partition_scheme_id = ps.data_space_id
        JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
        LEFT JOIN sys.partition_range_values AS rv ON pf.function_id = rv.function_id
                                                        AND dds.destination_id = CASE pf.boundary_value_on_right
                                                                                    WHEN 0 THEN rv.boundary_id
                                                                                    ELSE rv.boundary_id + 1
                                                                                END
        LEFT JOIN sys.indexes AS si ON dds.partition_scheme_id = si.data_space_id
        LEFT JOIN sys.partitions AS p ON si.object_id = p.object_id
                                            AND si.index_id = p.index_id
                                            AND dds.destination_id = p.partition_number
        LEFT JOIN sys.dm_db_partition_stats AS dbps ON p.object_id = dbps.object_id
                                                        AND p.partition_id = dbps.partition_id
GROUP BY ds.name ,
        p.partition_number ,
        pf.name ,
        pf.type_desc ,
        pf.fanout ,
        pf.boundary_value_on_right ,
        ps.name ,
        si.object_id ,
        rv.value;
GO

-- *********************************************************
RAISERROR('Create a view to see partition information by object', 0, 1) WITH NOWAIT
-- *********************************************************
IF OBJECT_ID('ph.vwObjectDetail', 'V') IS NOT NULL DROP VIEW ph.vwObjectDetail
GO
CREATE VIEW ph.vwObjectDetail	
AS
SELECT  SCHEMA_NAME(so.schema_id) AS schema_name ,
        OBJECT_NAME(p.object_id) AS object_name ,
        p.partition_number ,
        p.data_compression_desc ,
        dbps.row_count ,
        dbps.reserved_page_count * 8 / 1024. AS reserved_mb ,
        si.index_id ,
        CASE WHEN si.index_id = 0 THEN '(heap!)'
                ELSE si.name
        END AS index_name ,
        si.is_unique ,
        si.data_space_id ,
        mappedto.name AS mapped_to_name ,
        mappedto.type_desc AS mapped_to_type_desc ,
        partitionds.name AS partition_filegroup ,
        pf.name AS pf_name ,
        pf.type_desc AS pf_type_desc ,
        pf.fanout AS pf_fanout ,
        pf.boundary_value_on_right ,
        ps.name AS partition_scheme_name ,
        rv.value AS range_value
FROM    sys.partitions p
JOIN    sys.objects so
        ON p.object_id = so.object_id
            AND so.is_ms_shipped = 0
LEFT JOIN sys.dm_db_partition_stats AS dbps
        ON p.object_id = dbps.object_id
            AND p.partition_id = dbps.partition_id
JOIN    sys.indexes si
        ON p.object_id = si.object_id
            AND p.index_id = si.index_id
LEFT JOIN sys.data_spaces mappedto
        ON si.data_space_id = mappedto.data_space_id
LEFT JOIN sys.destination_data_spaces dds
        ON si.data_space_id = dds.partition_scheme_id
            AND p.partition_number = dds.destination_id
LEFT JOIN sys.data_spaces partitionds
        ON dds.data_space_id = partitionds.data_space_id
LEFT JOIN sys.partition_schemes AS ps
        ON dds.partition_scheme_id = ps.data_space_id
LEFT JOIN sys.partition_functions AS pf
        ON ps.function_id = pf.function_id
LEFT JOIN sys.partition_range_values AS rv
        ON pf.function_id = rv.function_id
            AND dds.destination_id = CASE pf.boundary_value_on_right
                                        WHEN 0 THEN rv.boundary_id
                                        ELSE rv.boundary_id + 1
                                    END
GO


-- *********************************************************
RAISERROR('Create a tally/numbers table with 100,000 rows', 0, 1) WITH NOWAIT
-- *********************************************************
IF OBJECT_ID('ph.tally', 'U') IS NOT NULL DROP TABLE ph.tally
;WITH    Pass0 AS ( SELECT   1 AS C UNION ALL SELECT   1), 
	Pass1 AS ( SELECT   1 AS C FROM     Pass0 AS A , Pass0 AS B),
	Pass2 AS ( SELECT   1 AS C FROM     Pass1 AS A , Pass1 AS B),
	Pass3 AS ( SELECT   1 AS C FROM     Pass2 AS A , Pass2 AS B),
	Pass4 AS ( SELECT   1 AS C FROM     Pass3 AS A , Pass3 AS B),
	Pass5 AS ( SELECT   1 AS C FROM     Pass4 AS A , Pass4 AS B),
	tally AS ( SELECT   row_number() OVER ( Order BY C ) AS N FROM Pass5 )
SELECT  N
	INTO    ph.tally
FROM    tally
WHERE   N <= 100000;	-- 4000000
GO

-- Check table
SELECT MinValue = MIN(N), MaxValue = MAX(N), CNT = COUNT(*) FROM ph.tally


RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *********************************************************
RAISERROR('3. CREATE OUR HERO, THE PARTITION FUNCTION', 0, 1) WITH NOWAIT
RAISERROR('		Creating the delimiters/boundaries we will use to break up the data', 0, 1) WITH NOWAIT
RAISERROR('		Cool point - you can use variables and functions', 0, 1) WITH NOWAIT
RAISERROR('		When typing dates to create a partition, use ODBC standard date format', 0, 1) WITH NOWAIT
RAISERROR('		We are using RANGE RIGHT (instead of LEFT)', 0, 1) WITH NOWAIT
-- *********************************************************

--Create the partition function: dailyPF
DECLARE @StartDay DATE=DATEADD(dd,-3,CAST(SYSDATETIME() AS DATE));
PRINT '@StartDay = ' + CAST(@StartDay AS VARCHAR(100))
CREATE PARTITION FUNCTION DailyPF (DATETIME2(0))
    AS RANGE RIGHT FOR VALUES
    (@StartDay, DATEADD(dd,1,@StartDay), DATEADD(dd,2,@StartDay),  
		DATEADD(dd,3,@StartDay), DATEADD(dd,4,@StartDay) );
GO


RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *********************************************************
RAISERROR('Let''s take a look at our partition function', 0, 1) WITH NOWAIT
-- *********************************************************
SELECT name,type_desc, fanout, boundary_value_on_right, create_date 
FROM sys.partition_functions;
GO

SELECT PF.[name], RV.boundary_id, RV.[value]
FROM sys.partition_range_values AS RV
 JOIN sys.partition_functions AS PF
  ON RV.function_id = PF.function_id
WHERE PF.[name] = 'DailyPF'

/*
1	|	2
1	|	2	|	3
1	|	2	|	3	|	4
*/


RAISERROR('Breakpoint!',20,1) WITH LOG;

-- *********************************************************
RAISERROR('4. SET UP SOME FILEGROUPS and FILES FOR OUR PARTITIONS TO LIVE ON.', 0, 1) WITH NOWAIT
RAISERROR('		In production they MIGHT be on different drives with the appropriate RAID and spindles (LUNs)', 0, 1) WITH NOWAIT
RAISERROR('		Number of filegroups = 1 + Number of boundary points', 0, 1) WITH NOWAIT
-- *********************************************************

ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG1
GO
ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG2
GO
ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG3
GO
ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG4
GO 
ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG5
GO
ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG6
GO


RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *********************************************************
RAISERROR('Add files to the filegroups', 0, 1) WITH NOWAIT
RAISERROR('		This is being done dynamically so it will work on different instances,but it makes some big assumptions!', 0, 1) WITH NOWAIT
-- *********************************************************
DECLARE @path NVARCHAR(256), @i TINYINT=1, @sql NVARCHAR(4000);
SELECT TOP 1 @path=LEFT(physical_name,LEN(physical_name)-4) 
FROM sys.database_files WHERE name='PartitionThis';

WHILE @i <= 6
BEGIN
	SET @sql=N'ALTER DATABASE PartitionThis ADD FILE (name=DailyF' + CAST(@i AS NCHAR(1))+', 
		 filename=''' +  @path + N'F'+ CAST(@i AS NCHAR(1))+'.ndf' + ''',
		 size=128MB, filegrowth=256MB) TO FILEGROUP DailyFG'+CAST(@i AS NCHAR(1))
	--show the command we're running
	RAISERROR (@sql,0,0) WITH NOWAIT
	
	--run it
	EXEC sp_executesql @sql;
	SET @i+=1;
END
GO


RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *********************************************************
RAISERROR('5. CREATE THE PARTITION SCHEME', 0, 1) WITH NOWAIT
RAISERROR('		This maps the filegroups to the partition function.', 0, 1) WITH NOWAIT
RAISERROR('		Create the partition scheme: dailyPS ', 0, 1) WITH NOWAIT
-- *********************************************************
CREATE PARTITION SCHEME DailyPS 
	AS PARTITION DailyPF
	TO (DailyFG1, DailyFG2, DailyFG3, DailyFG4, DailyFG5, DailyFG6);

--Look at how this is mapped out now
PRINT 'Today''s Date = ' + FORMAT(SYSDATETIME(),'yyyy-MM-dd') + '; ' + 'Seven (7) days ago = ' + FORMAT(DATEADD(dd,-7,SYSDATETIME()),'yyyy-MM-dd')
SELECT *
FROM ph.vwFileGroupDetail;
GO

-- *********************************************************
RAISERROR('You can use the $PARTITION function to see where data would fall', 0, 1) WITH NOWAIT
-- *********************************************************
SELECT $PARTITION.DailyPF( DATEADD(dd,-100,CAST(SYSDATETIME() AS DATETIME2(0)))) AS PartitionNumber100DaysAgo,
	$PARTITION.DailyPF( DATEADD(dd,-7,CAST(SYSDATETIME() AS DATETIME2(0)))) AS PartitionNumberSevenDaysAgo,
	$PARTITION.DailyPF( DATEADD(dd,-3,CAST(SYSDATETIME() AS DATETIME2(0)))) AS PartitionNumberThreeDaysAgo,
	$PARTITION.DailyPF( DATEADD(dd,-2,CAST(SYSDATETIME() AS DATETIME2(0)))) AS PartitionNumberTwoDaysAgo,
	$PARTITION.DailyPF( DATEADD(dd,1,CAST(SYSDATETIME() AS DATETIME2(0)))) AS PartitionNumberTomorrow,
	$PARTITION.DailyPF( DATEADD(dd,7,CAST(SYSDATETIME() AS DATETIME2(0)))) AS PartitionNumberNextWeek
GO


RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *********************************************************
RAISERROR('6. CREATE OBJECTS ON THE PARTITION SCHEME', 0, 1) WITH NOWAIT
RAISERROR('		Create a partitioned heap', 0, 1) WITH NOWAIT
-- *********************************************************

if OBJECT_ID('OrdersDaily','U') is null
CREATE TABLE OrdersDaily (
	OrderDate DATETIME2(0) NOT NULL,
	OrderId int IDENTITY NOT NULL,
	OrderName nvarchar(256) NOT NULL
) on DailyPS(OrderDate)
GO


RAISERROR('Breakpoint!',20,1) WITH LOG;

-- *********************************************************
RAISERROR('Let''s insert some rows!  We are leaving Partition 1 and Partition 6 empty on purpose. It''s a best practice to have empty partitions at each end.', 0, 1) WITH NOWAIT
-- *********************************************************

INSERT OrdersDaily(OrderDate, OrderName) 
SELECT DATEADD(ss, t.N, DATEADD(dd,-3,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))) AS OrderDate,
	CASE WHEN t.N % 3 = 0 THEN 'Robot' WHEN t.N % 4 = 0 THEN 'Badger'  ELSE 'Pen' END AS OrderName
FROM ph.tally AS t
WHERE N < = 10000;	--1000000
	
--Two days ago = 2000 rows
INSERT OrdersDaily(OrderDate, OrderName) 
SELECT DATEADD(ss, t.N, DATEADD(dd,-2,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))) AS OrderDate,
	CASE WHEN t.N % 3 = 0 THEN 'Flying Monkey' WHEN t.N % 4 = 0 THEN 'Junebug'  ELSE 'Pen' END AS OrderName
FROM ph.tally AS t
WHERE N < = 20000;

--Yesterday= 3000 rows
INSERT OrdersDaily(OrderDate, OrderName) 
SELECT DATEADD(ss, t.N, DATEADD(dd,-1,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))) AS OrderDate,
	CASE WHEN t.N % 2 = 0 THEN 'Turtle' WHEN t.N % 5 = 0 THEN 'Eraser'  ELSE 'Pen' END AS OrderName
FROM ph.tally AS t
WHERE N < = 30000;

--Today=  4000 rows
INSERT OrdersDaily(OrderDate, OrderName) 
SELECT DATEADD(ss, t.N, CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0))) AS OrderDate,
	CASE WHEN t.N % 3 = 0 THEN 'Lasso' WHEN t.N % 2 = 0 THEN 'Cattle Prod'  ELSE 'Pen' END AS OrderName
FROM ph.tally AS t
WHERE N < = 40000;
GO

RAISERROR('Breakpoint!',20,1) WITH LOG;

-- *********************************************************
RAISERROR('Let''s review our heap using the view/helper object we created', 0, 1) WITH NOWAIT
RAISERROR('		DailyFG1 and DailyFG6 are both empty as planned', 0, 1) WITH NOWAIT
-- *********************************************************
SELECT *
FROM ph.vwObjectDetail
WHERE object_name='OrdersDaily'
order by partition_number;
GO

-- *********************************************************
RAISERROR('7. Let''s add some indexes (a clustered index means it is a heap no longer!)', 0, 1) WITH NOWAIT
-- *********************************************************
ALTER TABLE OrdersDaily
ADD CONSTRAINT PKOrdersDaily
	PRIMARY KEY CLUSTERED(OrderDate,OrderId)
GO

-- *********************************************************
RAISERROR('An aligned NCI - We don''t have to specify the partition function.', 0, 1) WITH NOWAIT
-- *********************************************************
CREATE NONCLUSTERED INDEX NCOrderIdOrdersDaily 
	ON OrdersDaily(OrderId)
GO

-- *********************************************************
RAISERROR('An NCI that is NOT aligned', 0, 1) WITH NOWAIT
-- *********************************************************
CREATE NONCLUSTERED INDEX NCOrderNameOrdersDailyNonAligned 
	ON OrdersDaily(OrderName) ON [PRIMARY]
GO

RAISERROR('Breakpoint!',20,1) WITH LOG;

-- *********************************************************
RAISERROR('Look at the CI and NCs', 0, 1) WITH NOWAIT
-- *********************************************************
SELECT partition_number, row_count, range_value, reserved_mb, 
	index_id, index_name,mapped_to_name,mapped_to_type_desc, partition_filegroup, pf_name
FROM ph.vwObjectDetail
WHERE object_name='OrdersDaily'
order by index_name, partition_number

--compare to:
EXEC sp_helpindex OrdersDaily





RAISERROR('Breakpoint!',20,1) WITH LOG;

-- *********************************************************
RAISERROR('SWITCHING IN NEW PARTITIONS... Like lightning / DDL in action!', 0, 1) WITH NOWAIT
RAISERROR('		I want to load data for tomorrow and then switch it in.', 0, 1) WITH NOWAIT
RAISERROR('		First, add a filegroup.', 0, 1) WITH NOWAIT
-- *********************************************************
ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG7

-- *********************************************************
RAISERROR('Add a file for the filegroup.', 0, 1) WITH NOWAIT
-- *********************************************************
DECLARE @path NVARCHAR(256), @i TINYINT=7, @sql NVARCHAR(4000);
SELECT TOP 1 @path=LEFT(physical_name,LEN(physical_name)-4) 
FROM sys.database_files WHERE name='PartitionThis';

WHILE @i = 7
BEGIN
	SET @sql=N'ALTER DATABASE PartitionThis ADD FILE (name=DailyF' + CAST(@i AS NCHAR(1))+', 
		 filename=''' +  @path + N'F'+ CAST(@i AS NCHAR(1))+'.ndf' + ''',
		 size=128MB, filegrowth=256MB) TO FILEGROUP DailyFG'+CAST(@i AS NCHAR(1))
	--show the command we're running
	RAISERROR (@sql,0,0)
	
	--run it
	EXEC sp_executesql @sql
	SET @i+=1
END


RAISERROR('Breakpoint!',20,1) WITH LOG;

-- *********************************************************
RAISERROR('Create a staging table on our new filegroup (dailyFG6)', 0, 1) WITH NOWAIT
RAISERROR('		This simulates adding a days worth of data (imagine an ETL operation into a data warehouse)', 0, 1) WITH NOWAIT
RAISERROR('		Seeding the value at 100,001 to prevent overlap', 0, 1) WITH NOWAIT
-- *********************************************************
IF OBJECT_ID('dbo.OrdersDailyLoad', 'U') IS NOT NULL DROP TABLE dbo.OrdersDailyLoad
CREATE TABLE OrdersDailyLoad (
	OrderDate DATETIME2(0) NOT NULL,
	OrderId int IDENTITY (100001,1) NOT NULL,
	OrderName nvarchar(256) NOT NULL
) on [DailyFG6]
GO


-- *********************************************************
RAISERROR('Insert some records into our staging table', 0, 1) WITH NOWAIT
RAISERROR('		Tomorrow=50,000 rows', 0, 1) WITH NOWAIT
-- *********************************************************
INSERT OrdersDailyLoad(OrderDate, OrderName) 
SELECT 
	--DATEADD(SECOND, t.N, 
	--DATEADD(dd,1,CAST(SYSDATETIME() AS DATETIME2(0)))) AS OrderDate,
	DATEADD(ss, t.N, DATEADD(dd,1,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))) AS OrderDate,
	CASE WHEN t.N % 3 = 0 THEN 'Bow and Arrow' WHEN t.N % 2 = 0 
		THEN 'First Aid Kit'  
		ELSE 'Pen' 
	END AS OrderName
FROM ph.tally AS t
WHERE N < = 50000
GO

-- *********************************************************
RAISERROR('Create indexes on our staging table', 0, 1) WITH NOWAIT
-- *********************************************************
ALTER TABLE OrdersDailyLoad
ADD CONSTRAINT PKOrdersDailyLoad
	PRIMARY KEY CLUSTERED(OrderDate,OrderId)
GO

-- *********************************************************
RAISERROR('Create the aligned NC as well. It can have a different name.', 0, 1) WITH NOWAIT
RAISERROR('You must have at least the level of indexing/constraints in the table you are switching into as the DDL operation will SKIP THE CHECKS!', 0, 1) WITH NOWAIT
-- *********************************************************
CREATE NONCLUSTERED INDEX NCOrderIdOrdersDailyLoad ON OrdersDailyLoad(OrderId)
GO

-- *********************************************************
RAISERROR('Add an additional index to OrdersDailyLoad', 0, 1) WITH NOWAIT
RAISERROR('		This DOES NOT EXIST in the original table we will be loading into!', 0, 1) WITH NOWAIT
-- *********************************************************
CREATE NONCLUSTERED INDEX NCOrderIdOrderNameOrdersDailyLoad ON OrdersDailyLoad(OrderId, OrderName)
GO


-- *********************************************************
RAISERROR('Create two check constraints on the staging table.', 0, 1) WITH NOWAIT
RAISERROR('		This will ensure data fits in with the allowed range for the partition we want to put it in', 0, 1) WITH NOWAIT
RAISERROR('		Constraints WITH CHECK are required for switching in', 0, 1) WITH NOWAIT
RAISERROR('		Create one constraint for the "low end"', 0, 1) WITH NOWAIT
-- *********************************************************
-- Here is the range of the data
SELECT		MIN(OrderDate), MAX(OrderDate)
FROM		OrdersDailyLoad

--Create one constraint for the "low end"
DECLARE @tsql NVARCHAR(2000)=
'ALTER TABLE OrdersDailyLoad
WITH CHECK
ADD CONSTRAINT CKOrdersDailyLoad_LowEnd
CHECK (OrderDate >= ''' + 
	convert(CHAR(10),DATEADD(dd,1,CAST(SYSDATETIME() AS DATE))) + ''')'
--Display what we're running
RAISERROR (@tsql,0,0) WITH NOWAIT
--Run it
EXEC sp_executesql @tsql;
GO

-- *********************************************************
RAISERROR('		Create one constraint for the "high end"', 0, 1) WITH NOWAIT
-- *********************************************************
DECLARE @tsql NVARCHAR(2000)=
'ALTER TABLE OrdersDailyLoad
WITH CHECK
ADD CONSTRAINT CKOrdersDailyLoad_HighEnd
CHECK (OrderDate < ''' + 
	convert(CHAR(10),DATEADD(dd,2,CAST(SYSDATETIME() AS DATE))) + ''')'
--Display what we're running
RAISERROR (@tsql,0,0) WITH NOWAIT
--Run it
EXEC sp_executesql @tsql;


RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *********************************************************
RAISERROR('Set our new filegroup as "Next used" in our partition scheme', 0, 1) WITH NOWAIT
RAISERROR('This is how you add it to the partition scheme', 0, 1) WITH NOWAIT
-- *********************************************************
ALTER PARTITION SCHEME DailyPS
NEXT USED DailyFG7

-- *********************************************************
RAISERROR('This means DailyFG7 will receive any additional partition of a partitioned table or index as a result of an ALTER PARTITION FUNCTION statement.', 0, 1) WITH NOWAIT
RAISERROR('		Examine our partition function with assocated scheme, filegroups, and boundary points', 0, 1) WITH NOWAIT
-- *********************************************************
SELECT *
FROM ph.vwFileGroupDetail;
GO

RAISERROR('Breakpoint!',20,1) WITH LOG;

-- *********************************************************
RAISERROR('Add a new boundary point to our partition function. We already have an empty partition -- there''s no data right now in Partition #6.', 0, 1) WITH NOWAIT
RAISERROR('		But we always want to KEEP at least one empty partition at the high end, so we''re going to add another.', 0, 1) WITH NOWAIT
-- *********************************************************
PRINT DATEADD(dd,2,CAST(SYSDATETIME() AS DATE))
ALTER PARTITION FUNCTION DailyPF() 
SPLIT RANGE (DATEADD(dd,2,CAST(SYSDATETIME() AS DATE)))
GO

SELECT *
FROM ph.vwFileGroupDetail;
GO

RAISERROR('Breakpoint!',20,1) WITH LOG;

-- *********************************************************
--If you don't add a filegroup to the partition scheme first with NEXT USED, 
--you'll get the error:
--Msg 7707, Level 16, State 1, Line 2
--The associated partition function 'DailyPF' generates more partitions 
--than there are file groups mentioned in the scheme 'DailyPS'.

--But note that you *CAN* use a FileGroup for  more than one partition
--To do this,  you just set an existing one with NEXT USED.
-- *********************************************************

SELECT *
FROM ph.vwObjectDetail
WHERE object_name IN ('OrdersDaily','OrdersDailyLoad')
ORDER BY object_name, partition_number



-- *********************************************************
RAISERROR('SWITCH IN!', 0, 1) WITH NOWAIT
-- *********************************************************
ALTER TABLE OrdersDailyLoad
SWITCH TO OrdersDaily PARTITION 6




















-- *********************************************************
RAISERROR('Uh oh... We must disable (or drop) this non-aligned index to make switching work', 0, 1) WITH NOWAIT
-- *********************************************************
ALTER INDEX NCOrderNameOrdersDailyNonAligned ON OrdersDaily DISABLE;
GO

--Switch in!
ALTER TABLE OrdersDailyLoad
SWITCH TO OrdersDaily PARTITION 6;
GO


RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *********************************************************
RAISERROR('Let''s look at our partitioned table and loading table now...', 0, 1) WITH NOWAIT
RAISERROR('		Partition 6 should now have 5000 rows in it', 0, 1) WITH NOWAIT
RAISERROR('		Partition 1 and Partition 7 should be empty', 0, 1) WITH NOWAIT
-- *********************************************************
SELECT *
FROM ph.vwObjectDetail
WHERE object_name IN ('OrdersDaily','OrdersDailyLoad')
ORDER BY object_name, partition_number;
GO


RAISERROR('Breakpoint!',20,1) WITH LOG;


--Let's go ahead and drop the staging table
DROP TABLE OrdersDailyLoad;
GO












-- *********************************************************
RAISERROR('9. SWITCHING OUT OLD DATA', 0, 1) WITH NOWAIT
RAISERROR('		I have four right partition boundaries currently', 0, 1) WITH NOWAIT
RAISERROR('		I want to switch out my oldest data', 0, 1) WITH NOWAIT
-- *********************************************************
--Look at how this is mapped out now. 
--We want to get rid of our oldest 1000 rows.
--Those are sitting in Partition 2 which is on DailyFG2
SELECT *
FROM ph.vwFileGroupDetail
ORDER BY partition_number;
GO


-- *********************************************************
RAISERROR('Create a staging table to hold switched out data ', 0, 1) WITH NOWAIT
RAISERROR('		PUT THIS ON THE SAME FILEGROUP YOU''RE SWITCHING OUT OF', 0, 1) WITH NOWAIT
-- *********************************************************
CREATE TABLE OrdersDailyOut (
	OrderDate DATETIME2(0) NOT NULL,
	OrderId int IDENTITY NOT NULL,
	OrderName nvarchar(256) NOT NULL
) on [DailyFG2];
GO

-- *********************************************************
RAISERROR('Create the necessary indexes', 0, 1) WITH NOWAIT
-- *********************************************************
ALTER TABLE OrdersDailyOut
ADD CONSTRAINT PKOrdersDailyOut
	PRIMARY KEY CLUSTERED(OrderDate,OrderId);
GO

RAISERROR('Breakpoint!',20,1) WITH LOG;

-- *********************************************************
RAISERROR('Switching OUT!', 0, 1) WITH NOWAIT
-- *********************************************************
ALTER TABLE OrdersDaily
SWITCH PARTITION 2 TO OrdersDailyOut;
GO

RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *********************************************************
RAISERROR('Look at our switch OUT table', 0, 1) WITH NOWAIT
RAISERROR('		OrdersDailyOut should have 1000 rows', 0, 1) WITH NOWAIT
RAISERROR('		Partition 1 and Partition 2 of OrdersDaily should have 0 rows', 0, 1) WITH NOWAIT
-- *********************************************************
SELECT *
FROM ph.vwObjectDetail
WHERE object_name IN ('OrdersDaily','OrdersDailyOut')
ORDER BY object_name DESC, partition_number;
GO

RAISERROR('Breakpoint!',20,1) WITH LOG;

-- *********************************************************
--Note: when switching out to an empty table, we needed to add the clustered index. 
--However, we did NOT need to add NCIs or a check constraint. (Whew!)
-- *********************************************************

-- *********************************************************
RAISERROR('We want to keep an empty partition on DailyFG1', 0, 1) WITH NOWAIT
RAISERROR('		But we want to remove the empty partition on DailyFG2 (currently Partition 2)', 0, 1) WITH NOWAIT
RAISERROR('		Programmatically find the boundary point to merge. This is done so we don''t have to hard code dates in the script', 0, 1) WITH NOWAIT
-- *********************************************************
DECLARE @MergeBoundaryPoint DATETIME2(0), @msg NVARCHAR(2000);
SELECT @MergeBoundaryPoint = CAST(MIN(rv.value) AS DATETIME2(0))
FROM sys.partition_functions  pf
JOIN sys.partition_range_values rv ON pf.function_id=rv.function_id
where pf.name='DailyPF'

IF (
	SELECT COUNT(*)
	FROM dbo.OrdersDaily
	WHERE OrderDate < dateadd(DAY, 1, @MergeBoundaryPoint)
) =0
BEGIN
	SET @msg='No records found, merging boundary point ' 
		+ CAST(@MergeBoundaryPoint AS CHAR(10)) + '.'
	RAISERROR (@msg,0,0)
	ALTER PARTITION FUNCTION DailyPF ()
		MERGE RANGE ( @MergeBoundaryPoint )
END
ELSE
BEGIN
	SET @msg='ERROR: Records exist around boundary point ' 
		+ CAST(@MergeBoundaryPoint AS CHAR(10)) + '. Not merging.'
	RAISERROR (@msg,16,1)
END



RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *********************************************************
RAISERROR('Look at how this is mapped out after switch-out and merging boundary points.', 0, 1) WITH NOWAIT
RAISERROR('		DailyFG1 should be present and have 0 rows.', 0, 1) WITH NOWAIT
RAISERROR('		No partitions should be mapped to DailyFG2 our boundary point merge got rid of it. (It was empty.)', 0, 1) WITH NOWAIT
-- *********************************************************
SELECT *
FROM ph.vwFileGroupDetail;
GO


RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *********************************************************
RAISERROR('Cleanup - drop Switch Out table, look at state after cleanup', 0, 1) WITH NOWAIT
-- *********************************************************
DROP TABLE OrdersDailyOut;
GO

SELECT *
FROM ph.vwObjectDetail
WHERE object_name IN ('OrdersDaily')
ORDER BY object_name DESC, partition_number;
GO


RAISERROR('Breakpoint!',20,1) WITH LOG;





-- *********************************************************
RAISERROR('9. Full Table Switch', 0, 1) WITH NOWAIT
RAISERROR('		A non-partitioned table is still 1 BIG PARTITION!', 0, 1) WITH NOWAIT
RAISERROR('		I want to TRUNCate and REPOP without the user feeling the pain of the insert', 0, 1) WITH NOWAIT
-- *********************************************************

-- *******************************
RAISERROR('Create two tables - 1 with data and 1 without', 0, 1) WITH NOWAIT
-- *******************************
IF OBJECT_ID('dbo.OrdersDailyNPStaging', 'U') IS NOT NULL DROP TABLE dbo.OrdersDailyNPStaging
SELECT		*
	INTO	dbo.OrdersDailyNPStaging
FROM		dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily
UNION ALL SELECT * FROM	dbo.OrdersDaily


IF OBJECT_ID('dbo.OrdersDailyNPConsumption', 'U') IS NOT NULL DROP TABLE dbo.OrdersDailyNPConsumption
SELECT		*
	INTO	dbo.OrdersDailyNPConsumption
FROM		dbo.OrdersDaily
WHERE		1=0


RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *******************************
RAISERROR('Use our helper view to check on our tables', 0, 1) WITH NOWAIT
-- *******************************

SELECT *
FROM ph.vwObjectDetail
WHERE object_name LIKE 'OrdersDailyNP%'


RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *******************************
RAISERROR('Perform the SWITCH (Partition switch)', 0, 1) WITH NOWAIT
-- *******************************

ALTER TABLE dbo.OrdersDailyNPStaging SWITCH TO dbo.OrdersDailyNPConsumption

SELECT *
FROM ph.vwObjectDetail
WHERE object_name LIKE 'OrdersDailyNP%'


RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *******************************
RAISERROR('Apply indexes and try again (doing it the hard way the first time)', 0, 1) WITH NOWAIT
-- *******************************
CREATE CLUSTERED INDEX clust on dbo.OrdersDailyNPStaging(OrderDate, OrderID)
CREATE CLUSTERED INDEX clust on dbo.OrdersDailyNPConsumption(OrderDate, OrderID)

INSERT INTO dbo.OrdersDailyNPStaging
SELECT TOP 2699999* FROM dbo.OrdersDailyNPConsumption	-- @ 2.7 million records

SELECT *
FROM ph.vwObjectDetail
WHERE object_name LIKE 'OrdersDailyNP%'

RAISERROR('Breakpoint!',20,1) WITH LOG;


-- *******************************
RAISERROR('Perform 2 fast DDL operations to clear out the target and then populate the table', 0, 1) WITH NOWAIT
-- *******************************
TRUNCATE TABLE dbo.OrdersDailyNPConsumption
ALTER TABLE dbo.OrdersDailyNPStaging SWITCH TO dbo.OrdersDailyNPConsumption

SELECT *
FROM ph.vwObjectDetail
WHERE object_name LIKE 'OrdersDailyNP%'