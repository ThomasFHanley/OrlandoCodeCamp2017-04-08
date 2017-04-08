USE [AdventureWorks2014]
GO

SET NOCOUNT ON
GO

-- ***************************************************************************************************************************************************************************
-- ***************************************************************************************************************************************************************************
RAISERROR('Single table / no vertical partitioning', 0, 1) WITH NOWAIT
-- ***************************************************************************************************************************************************************************

-- ******************************************************************************************************************
RAISERROR('Creating CodeCamp.Reports table', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
IF OBJECT_ID('CodeCamp.vwReports', 'V') IS NOT NULL DROP VIEW CodeCamp.vwReports
IF OBJECT_ID('CodeCamp.ReportsDesc', 'U') IS NOT NULL DROP TABLE CodeCamp.ReportsDesc
IF OBJECT_ID('CodeCamp.ReportsData', 'U') IS NOT NULL DROP TABLE CodeCamp.ReportsData
IF OBJECT_ID('CodeCamp.Reports', 'U') IS NOT NULL DROP TABLE CodeCamp.Reports
CREATE TABLE CodeCamp.Reports
(
		ReportID INT IDENTITY (1,1) NOT NULL
	,	ReportName VARCHAR (100) NOT NULL
	,	ReportNumber VARCHAR (20) NOT NULL
	,	ReportDescription VARCHAR (8000) NOT NULL
	CONSTRAINT Reports_PK PRIMARY KEY CLUSTERED (ReportID)
)

-- *********************************************************
RAISERROR('Populating CodeCamp.Reports table', 0, 1) WITH NOWAIT
-- *********************************************************
DECLARE @i int
SET @i = 1
 
BEGIN TRAN
WHILE @i < 100000 
BEGIN
	INSERT INTO CodeCamp.Reports
	(
			ReportName
		,	ReportNumber
		,	ReportDescription
	)
	VALUES
	(
			'ReportName' + CAST(@i AS VARCHAR)
		,	CONVERT (varchar (20), @i)
		,	REPLICATE ('Report'+ CAST(@i AS VARCHAR), 1000)
	)
	SET @i=@i+1
END
COMMIT TRAN
GO




SELECT TOP 100 * FROM CodeCamp.Reports

-- *********************************************************
RAISERROR('Clean Buffers & Cache', 0, 1) WITH NOWAIT
-- *********************************************************
DBCC DROPCLEANBUFFERS;
DBCC FREEPROCCACHE;
-- *******************************

-- ******************************************************************************************************************
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
RAISERROR('Query the one (1) LARGE non-partitioned table', 0, 1) WITH NOWAIT
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
SET STATISTICS IO ON
SET STATISTICS TIME ON

SELECT		RPT.ReportID, RPT.ReportName, RPT.ReportNumber
FROM		CodeCamp.Reports RPT
WHERE		RPT.ReportNumber LIKE '%33%'

SET STATISTICS IO OFF
SET STATISTICS TIME OFF





-- ***************************************************************************************************************************************************************************
-- ***************************************************************************************************************************************************************************
RAISERROR('Two tables / vertical partitioning', 0, 1) WITH NOWAIT
-- ***************************************************************************************************************************************************************************

 -- ******************************************************************************************************************
RAISERROR('Creating CodeCamp.ReportsData table', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
IF OBJECT_ID('CodeCamp.ReportsData', 'U') IS NOT NULL DROP TABLE CodeCamp.ReportsData
CREATE TABLE CodeCamp.ReportsData
(
		ReportID INT FOREIGN KEY REFERENCES CodeCamp.Reports (ReportID)
	,	ReportName		VARCHAR (100) NOT NULL
	,	ReportNumber	VARCHAR (20) NOT NULL
	CONSTRAINT ReportsData_PK PRIMARY KEY CLUSTERED (ReportID)
)

 -- *********************************************************
RAISERROR('Populating CodeCamp.ReportsData table', 0, 1) WITH NOWAIT
-- *********************************************************
INSERT INTO CodeCamp.ReportsData
(
    ReportID,
    ReportName,
    ReportNumber
)
SELECT		RPT.ReportID
		,	RPT.ReportName
		,	RPT.ReportNumber 
FROM		CodeCamp.Reports RPT

-- ******************************************************************************************************************
RAISERROR('Creating CodeCamp.ReportsDesc table', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
IF OBJECT_ID('CodeCamp.ReportsDesc', 'U') IS NOT NULL DROP TABLE CodeCamp.ReportsDesc
CREATE TABLE CodeCamp.ReportsDesc 
(		ReportID INT FOREIGN KEY REFERENCES CodeCamp.Reports (ReportID)
	,	ReportDescription VARCHAR(8000) NOT NULL
	CONSTRAINT ReportsDesc_PK PRIMARY KEY CLUSTERED (ReportID)
 )

  -- *********************************************************
RAISERROR('Populating CodeCamp.ReportsDesc table', 0, 1) WITH NOWAIT
-- *********************************************************
INSERT INTO CodeCamp.ReportsDesc
(
    ReportID,
    ReportDescription
)
SELECT		RPT.ReportID
		,	RPT.ReportDescription
FROM		CodeCamp.Reports RPT

-- ******************************************************************************************************************
RAISERROR('Creating CodeCamp.vwReports view (view that takes vertically partitioned data and combines it back together)', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
IF OBJECT_ID('CodeCamp.vwReports', 'V') IS NOT NULL DROP VIEW CodeCamp.vwReports
GO
CREATE VIEW CodeCamp.vwReports
AS
SELECT		RDA.ReportID
		,	RDA.ReportName
		,	RDA.ReportNumber
		,	RDE.ReportDescription
FROM		CodeCamp.ReportsData RDA
			INNER JOIN CodeCamp.ReportsDesc RDE
				ON	RDA.ReportID = RDE.ReportID
GO





-- ******************************************************************************************************************
-- ******************************************************************************************************************
-- *******************************
RAISERROR('Clean Buffers & Cache', 0, 1) WITH NOWAIT
-- *********************************************************
DBCC DROPCLEANBUFFERS;
DBCC FREEPROCCACHE;
-- *******************************

-- ******************************************************************************************************************
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
RAISERROR('Query one of the new tables split out / vertically partitioned', 0, 1) WITH NOWAIT
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
SET STATISTICS IO ON
SET STATISTICS TIME ON

SELECT		RDA.ReportID, RDA.ReportName, RDA.ReportNumber
FROM		CodeCamp.ReportsData RDA
WHERE		RDA.ReportNumber LIKE '%33%'

SET STATISTICS IO OFF
SET STATISTICS TIME OFF




-- ******************************************************************************************************************
-- ******************************************************************************************************************
-- *******************************
RAISERROR('Clean Buffers & Cache', 0, 1) WITH NOWAIT
-- *******************************
DBCC DROPCLEANBUFFERS;
DBCC FREEPROCCACHE;
-- *******************************

 -- ******************************************************************************************************************
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
RAISERROR('Query the view using the vertically partitioned tables / Only select from 1 table', 0, 1) WITH NOWAIT
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
SET STATISTICS IO ON
SET STATISTICS TIME ON

SELECT		RDA.ReportID, RDA.ReportName, RDA.ReportNumber
FROM		CodeCamp.vwReports RDA
WHERE		RDA.ReportNumber LIKE '%33%'

SET STATISTICS IO OFF
SET STATISTICS TIME OFF



-- ***************************************************************************************************************************************************************************
-- ***************************************************************************************************************************************************************************
RAISERROR('Having data span databases, storage mediums, or filegroups', 0, 1) WITH NOWAIT
-- ***************************************************************************************************************************************************************************

-- *********************************************************
RAISERROR('Create table in new database and on a different filegroup (created an additional filegroup even though it is another database, foreign key cannot span databases - use a trigger instead)', 0, 1) WITH NOWAIT
-- *********************************************************
USE AnotherDatabase
GO

IF OBJECT_ID('CodeCamp.ReportsDesc', 'U') IS NOT NULL DROP TABLE CodeCamp.ReportsDesc
CREATE TABLE CodeCamp.ReportsDesc 
(		ReportID INT -- FOREIGN KEY REFERENCES CodeCamp.Reports (ReportID)
	,	ReportDescription VARCHAR(8000) NOT NULL
	CONSTRAINT ReportsDesc_PK PRIMARY KEY CLUSTERED (ReportID)
 ) ON AnotherFileGroup

SELECT	o.[name], o.[type], i.[name], i.[index_id], f.[name] 
FROM	sys.indexes i
		INNER JOIN sys.filegroups f
			ON i.data_space_id = f.data_space_id
		INNER JOIN sys.all_objects o
			ON i.[object_id] = o.[object_id] WHERE i.data_space_id = f.data_space_id
			AND o.type = 'U' -- User Created Tables

-- *********************************************************
RAISERROR('Populating CodeCamp.ReportsDesc table', 0, 1) WITH NOWAIT
-- *********************************************************
INSERT INTO AnotherDatabase.CodeCamp.ReportsDesc
(
    ReportID,
    ReportDescription
)
SELECT		RPT.ReportID
		,	RPT.ReportDescription
FROM		AdventureWorks2014.CodeCamp.Reports RPT



-- ******************************************************************************************************************
RAISERROR('Creating CodeCamp.vwReportsAcrossDatabases view (view that takes vertically partitioned data and combines it back together)', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
USE AdventureWorks2014
GO

IF OBJECT_ID('CodeCamp.vwReportsAcrossDatabases', 'V') IS NOT NULL DROP VIEW CodeCamp.vwReportsAcrossDatabases
GO
CREATE VIEW CodeCamp.vwReportsAcrossDatabases
AS
SELECT		RDA.ReportID
		,	RDA.ReportName
		,	RDA.ReportNumber
		,	RDE.ReportDescription
FROM		AdventureWorks2014.CodeCamp.ReportsData RDA
			INNER JOIN AnotherDatabase.CodeCamp.ReportsDesc RDE
				ON	RDA.ReportID = RDE.ReportID
GO




-- ******************************************************************************************************************
-- ******************************************************************************************************************
-- *******************************
RAISERROR('Clean Buffers & Cache', 0, 1) WITH NOWAIT
-- *******************************
DBCC DROPCLEANBUFFERS;
DBCC FREEPROCCACHE;
-- *******************************

 -- ******************************************************************************************************************
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
RAISERROR('Query the view using the vertically partitioned tables / Only select from 1 table', 0, 1) WITH NOWAIT
RAISERROR('------------------------------------------------------------------', 0, 1) WITH NOWAIT
-- ******************************************************************************************************************
SET STATISTICS IO ON
SET STATISTICS TIME ON

SELECT		RAD.ReportID, RAD.ReportName, RAD.ReportNumber
FROM		CodeCamp.vwReportsAcrossDatabases RAD
WHERE		RAD.ReportNumber LIKE '%33%'

SET STATISTICS IO OFF
SET STATISTICS TIME OFF