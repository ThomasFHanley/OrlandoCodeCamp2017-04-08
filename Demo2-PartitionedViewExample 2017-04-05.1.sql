USE [AdventureWorks2014]
GO

SET NOCOUNT ON
GO

-- ***************************************************************************************************************************************************************************
-- ***************************************************************************************************************************************************************************
RAISERROR('Horizontal Partitioning via PARTITION VIEW', 0, 1) WITH NOWAIT
-- ***************************************************************************************************************************************************************************

-- *********************************************************
RAISERROR('Base Data we are working with', 0, 1) WITH NOWAIT
-- *********************************************************
SELECT		COUNT(*)	--2,500,000
FROM		CodeCamp.RandomDataWithDates RDD

SELECT		TOP 1000 *
FROM		CodeCamp.RandomDataWithDates RDD

RAISERROR('Breakpoint!',20,1) WITH LOG;

-- ******************************************************************************************************************
RAISERROR('Split the data out into separate tables by month and year', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
-- 2015
IF OBJECT_ID('CodeCamp.RandomData2015Q2', 'U') IS NOT NULL DROP TABLE CodeCamp.RandomData2015Q2
SELECT		*
	INTO	CodeCamp.RandomData2015Q2
FROM		CodeCamp.RandomDataWithDates RDD
WHERE		RDD.SomeDateTime >= '2015-04-01' AND RDD.SomeDateTime < '2015-07-01'

IF OBJECT_ID('CodeCamp.RandomData2015Q3', 'U') IS NOT NULL DROP TABLE CodeCamp.RandomData2015Q3
SELECT		*
	INTO	CodeCamp.RandomData2015Q3
FROM		CodeCamp.RandomDataWithDates RDD
WHERE		RDD.SomeDateTime >= '2015-07-01' AND RDD.SomeDateTime < '2015-10-01'

IF OBJECT_ID('CodeCamp.RandomData2015Q4', 'U') IS NOT NULL DROP TABLE CodeCamp.RandomData2015Q4
SELECT		*
	INTO	CodeCamp.RandomData2015Q4
FROM		CodeCamp.RandomDataWithDates RDD
WHERE		RDD.SomeDateTime >= '2015-10-01' AND RDD.SomeDateTime < '2016-01-01'

-- 2016
IF OBJECT_ID('CodeCamp.RandomData2016Q1', 'U') IS NOT NULL DROP TABLE CodeCamp.RandomData2016Q1
SELECT		*
	INTO	CodeCamp.RandomData2016Q1
FROM		CodeCamp.RandomDataWithDates RDD
WHERE		RDD.SomeDateTime >= '2016-01-01' AND RDD.SomeDateTime < '2016-04-01'

IF OBJECT_ID('CodeCamp.RandomData2016Q2', 'U') IS NOT NULL DROP TABLE CodeCamp.RandomData2016Q2
SELECT		*
	INTO	CodeCamp.RandomData2016Q2
FROM		CodeCamp.RandomDataWithDates RDD
WHERE		RDD.SomeDateTime >= '2016-04-01' AND RDD.SomeDateTime < '2016-07-01'

IF OBJECT_ID('CodeCamp.RandomData2016Q3', 'U') IS NOT NULL DROP TABLE CodeCamp.RandomData2016Q3
SELECT		*
	INTO	CodeCamp.RandomData2016Q3
FROM		CodeCamp.RandomDataWithDates RDD
WHERE		RDD.SomeDateTime >= '2016-07-01' AND RDD.SomeDateTime < '2016-10-01'

IF OBJECT_ID('CodeCamp.RandomData2016Q4', 'U') IS NOT NULL DROP TABLE CodeCamp.RandomData2016Q4
SELECT		*
	INTO	CodeCamp.RandomData2016Q4
FROM		CodeCamp.RandomDataWithDates RDD
WHERE		RDD.SomeDateTime >= '2016-10-01' AND RDD.SomeDateTime < '2017-01-01'

-- 2017
IF OBJECT_ID('CodeCamp.RandomData2017Q1', 'U') IS NOT NULL DROP TABLE CodeCamp.RandomData2017Q1
SELECT		*
	INTO	CodeCamp.RandomData2017Q1
FROM		CodeCamp.RandomDataWithDates RDD
WHERE		RDD.SomeDateTime >= '2017-01-01' AND RDD.SomeDateTime < '2017-04-01'

IF OBJECT_ID('CodeCamp.RandomData2017Q2', 'U') IS NOT NULL DROP TABLE CodeCamp.RandomData2017Q2
SELECT		*
	INTO	CodeCamp.RandomData2017Q2
FROM		CodeCamp.RandomDataWithDates RDD
WHERE		RDD.SomeDateTime >= '2017-04-01' AND RDD.SomeDateTime < '2017-07-01'


-- ******************************************************************************************************************
RAISERROR('Create a basic partitioned view', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
IF OBJECT_ID('CodeCamp.vwRandomDataPV', 'V') IS NOT NULL DROP VIEW CodeCamp.vwRandomDataPV
GO
CREATE VIEW CodeCamp.vwRandomDataPV
AS
SELECT		*
FROM		CodeCamp.RandomData2015Q2
UNION ALL -- UNION ALL, not UNION!!!!
SELECT		*
FROM		CodeCamp.RandomData2015Q3
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2015Q4
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2016Q1
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2016Q2
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2016Q3
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2016Q4
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2017Q1
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2017Q2
GO

-- ******************************************************************************************************************
RAISERROR('Compare the data to make sure we covered all of the data', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
SELECT		COUNT(*)
FROM		CodeCamp.vwRandomDataPV CMB

-- Are there any differences?
SELECT		*
FROM		CodeCamp.RandomDataWithDates RDD
EXCEPT
SELECT		*
FROM		CodeCamp.vwRandomDataPV CMB

-- Do both sides
SELECT		*
FROM		CodeCamp.vwRandomDataPV CMB
EXCEPT
SELECT		*
FROM		CodeCamp.RandomDataWithDates RDD

RAISERROR('Breakpoint!',20,1) WITH LOG;

-- *********************************************************
RAISERROR('Clean Buffers & Cache', 0, 1) WITH NOWAIT
-- *********************************************************
DBCC DROPCLEANBUFFERS;
DBCC FREEPROCCACHE;
-- *******************************

-- ******************************************************************************************************************
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
RAISERROR('Query the original table', 0, 1) WITH NOWAIT
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
SET STATISTICS IO ON
SET STATISTICS TIME ON

SELECT		COUNT(*)
		,	SUM(SomeNumeric)
FROM		CodeCamp.RandomDataWithDates RDD
WHERE		RDD.SomeDateTime BETWEEN '2015-07-14 1:35:42' AND '2015-09-04 23:21:14'
			OR	RDD.SomeDateTime BETWEEN '2016-11-25 16:12:42' AND '2016-12-21 02:18:58'

SET STATISTICS IO OFF
SET STATISTICS TIME OFF


-- *********************************************************
RAISERROR('Clean Buffers & Cache', 0, 1) WITH NOWAIT
-- *********************************************************
DBCC DROPCLEANBUFFERS;
DBCC FREEPROCCACHE;
-- *******************************

-- ******************************************************************************************************************
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
RAISERROR('Query the basic partitioned view', 0, 1) WITH NOWAIT
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
SET STATISTICS IO ON
SET STATISTICS TIME ON

SELECT		COUNT(*)
		,	SUM(SomeNumeric)
FROM		CodeCamp.vwRandomDataPV RDD
WHERE		RDD.SomeDateTime BETWEEN '2015-07-14 1:35:42' AND '2015-09-04 23:21:14'
			OR	RDD.SomeDateTime BETWEEN '2016-11-25 16:12:42' AND '2016-12-21 02:18:58'

SET STATISTICS IO OFF
SET STATISTICS TIME OFF

-- An improvement on CPU time, but not an improvement on execution time
RAISERROR('Breakpoint!',20,1) WITH LOG;

-- ******************************************************************************************************************
RAISERROR('Create a partitioned view with WHERE clause (to HINT the optimizer)', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
IF OBJECT_ID('CodeCamp.vwRandomDataPVWithWhere', 'V') IS NOT NULL DROP VIEW CodeCamp.vwRandomDataPVWithWhere
GO
CREATE VIEW CodeCamp.vwRandomDataPVWithWhere
AS
SELECT		*
FROM		CodeCamp.RandomData2015Q2
WHERE		SomeDateTime >= '2015-04-01' AND SomeDateTime < '2015-07-01'
UNION ALL -- UNION ALL, not UNION!!!!
SELECT		*
FROM		CodeCamp.RandomData2015Q3
WHERE		SomeDateTime >= '2015-07-01' AND SomeDateTime < '2015-10-01'
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2015Q4
WHERE		SomeDateTime >= '2015-10-01' AND SomeDateTime < '2016-01-01'
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2016Q1
WHERE		SomeDateTime >= '2016-01-01' AND SomeDateTime < '2016-04-01'
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2016Q2
WHERE		SomeDateTime >= '2015-04-01' AND SomeDateTime < '2016-07-01'
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2016Q3
WHERE		SomeDateTime >= '2016-07-01' AND SomeDateTime < '2016-10-01'
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2016Q4
WHERE		SomeDateTime >= '2016-10-01' AND SomeDateTime < '2017-01-01'
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2017Q1
WHERE		SomeDateTime >= '2017-01-01' AND SomeDateTime < '2017-04-01'
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2017Q2
WHERE		SomeDateTime >= '2017-04-01' AND SomeDateTime < '2017-07-01'
GO

-- *********************************************************
RAISERROR('Clean Buffers & Cache', 0, 1) WITH NOWAIT
-- *********************************************************
DBCC DROPCLEANBUFFERS;
DBCC FREEPROCCACHE;
-- *******************************

-- ******************************************************************************************************************
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
RAISERROR('Query the partitioned view with WHERE clauses', 0, 1) WITH NOWAIT
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
SET STATISTICS IO ON
SET STATISTICS TIME ON

SELECT		COUNT(*)
		,	SUM(SomeNumeric)
FROM		CodeCamp.vwRandomDataPVWithWhere RDD
WHERE		RDD.SomeDateTime BETWEEN '2015-07-14 1:35:42' AND '2015-09-04 23:21:14'
			OR	RDD.SomeDateTime BETWEEN '2016-11-25 16:12:42' AND '2016-12-21 02:18:58'

SET STATISTICS IO OFF
SET STATISTICS TIME OFF

-- An improvement on CPU time AND execution time
RAISERROR('Breakpoint!',20,1) WITH LOG;

-- ******************************************************************************************************************
RAISERROR('Create check constraints on the tables (to HINT the optimizer)', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
ALTER TABLE CodeCamp.RandomData2015Q2
    ADD CONSTRAINT CHK_SomeDateTime_2015Q2
        CHECK ([SomeDateTime]>={d '2015-04-01'} AND [SomeDateTime]<{d '2015-07-01'});

ALTER TABLE CodeCamp.RandomData2015Q3
    ADD CONSTRAINT CHK_SomeDateTime_2015Q3
        CHECK ([SomeDateTime]>={d '2015-07-01'} AND [SomeDateTime]<{d '2015-10-01'});

ALTER TABLE CodeCamp.RandomData2015Q4
    ADD CONSTRAINT CHK_SomeDateTime_2015Q4
        CHECK ([SomeDateTime]>={d '2015-10-01'} AND [SomeDateTime]<{d '2016-01-01'});

ALTER TABLE CodeCamp.RandomData2016Q1
    ADD CONSTRAINT CHK_SomeDateTime_2016Q1
        CHECK ([SomeDateTime]>={d '2016-01-01'} AND [SomeDateTime]<{d '2016-04-01'});

ALTER TABLE CodeCamp.RandomData2016Q2
    ADD CONSTRAINT CHK_SomeDateTime_2016Q2
        CHECK ([SomeDateTime]>={d '2016-04-01'} AND [SomeDateTime]<{d '2016-07-01'});

ALTER TABLE CodeCamp.RandomData2016Q3
    ADD CONSTRAINT CHK_SomeDateTime_2016Q3
        CHECK ([SomeDateTime]>={d '2016-07-01'} AND [SomeDateTime]<{d '2016-10-01'});

ALTER TABLE CodeCamp.RandomData2016Q4
    ADD CONSTRAINT CHK_SomeDateTime_2016Q4
        CHECK ([SomeDateTime]>={d '2016-10-01'} AND [SomeDateTime]<{d '2017-01-01'});

ALTER TABLE CodeCamp.RandomData2017Q1
    ADD CONSTRAINT CHK_SomeDateTime_2017Q1
        CHECK ([SomeDateTime]>={d '2017-01-01'} AND [SomeDateTime]<{d '2017-04-01'});

ALTER TABLE CodeCamp.RandomData2017Q2
    ADD CONSTRAINT CHK_SomeDateTime_2017Q2
        CHECK ([SomeDateTime]>={d '2017-04-01'} AND [SomeDateTime]<{d '2017-07-01'});




-- *********************************************************
RAISERROR('Clean Buffers & Cache', 0, 1) WITH NOWAIT
-- *********************************************************
DBCC DROPCLEANBUFFERS;
DBCC FREEPROCCACHE;
-- *******************************

-- ******************************************************************************************************************
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
RAISERROR('Query the basic partitioned view with tables that have CHECK CONSTRAINTS applied', 0, 1) WITH NOWAIT
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
SET STATISTICS IO ON
SET STATISTICS TIME ON

SELECT		COUNT(*)
		,	SUM(SomeNumeric)
FROM		CodeCamp.vwRandomDataPV RDD
WHERE		RDD.SomeDateTime BETWEEN '2015-07-14 1:35:42' AND '2015-09-04 23:21:14'
			OR	RDD.SomeDateTime BETWEEN '2016-11-25 16:12:42' AND '2016-12-21 02:18:58'

SET STATISTICS IO OFF
SET STATISTICS TIME OFF


RAISERROR('Breakpoint!',20,1) WITH LOG;

-- ******************************************************************************************************************
RAISERROR('Roll data off of the partitioned view (Nix 2015)', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
IF OBJECT_ID('CodeCamp.vwRandomDataPVMinus2015', 'V') IS NOT NULL DROP VIEW CodeCamp.vwRandomDataPVMinus2015
GO
CREATE VIEW CodeCamp.vwRandomDataPVMinus2015
AS
SELECT		*
FROM		CodeCamp.RandomData2016Q1
WHERE		SomeDateTime >= '2016-01-01' AND SomeDateTime < '2016-04-01'
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2016Q2
WHERE		SomeDateTime >= '2015-04-01' AND SomeDateTime < '2016-07-01'
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2016Q3
WHERE		SomeDateTime >= '2016-07-01' AND SomeDateTime < '2016-10-01'
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2016Q4
WHERE		SomeDateTime >= '2016-10-01' AND SomeDateTime < '2017-01-01'
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2017Q1
WHERE		SomeDateTime >= '2017-01-01' AND SomeDateTime < '2017-04-01'
UNION ALL
SELECT		*
FROM		CodeCamp.RandomData2017Q2
WHERE		SomeDateTime >= '2017-04-01' AND SomeDateTime < '2017-07-01'
GO

-- *********************************************************
RAISERROR('Clean Buffers & Cache', 0, 1) WITH NOWAIT
-- *********************************************************
DBCC DROPCLEANBUFFERS;
DBCC FREEPROCCACHE;
-- *******************************

-- ******************************************************************************************************************
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
RAISERROR('Query the basic partitioned view CodeCamp.vwRandomDataPVMinus2015 (2015 has been removed)', 0, 1) WITH NOWAIT
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
SET STATISTICS IO ON
SET STATISTICS TIME ON

SELECT		COUNT(*)
		,	SUM(SomeNumeric)
FROM		CodeCamp.vwRandomDataPVMinus2015 RDD
WHERE		RDD.SomeDateTime BETWEEN '2015-07-14 1:35:42' AND '2015-09-04 23:21:14'
			OR	RDD.SomeDateTime BETWEEN '2016-11-25 16:12:42' AND '2016-12-21 02:18:58'

SET STATISTICS IO OFF
SET STATISTICS TIME OFF