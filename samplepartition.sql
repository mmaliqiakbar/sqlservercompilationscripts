
---create sample database
USE master
GO
DROP DATABASE PARTITIONDB

CREATE DATABASE PARTITIONDB

---create filegroup for partition target
ALTER DATABASE PARTITIONDB
ADD FILEGROUP NOV2022
GO

ALTER DATABASE PARTITIONDB
ADD FILEGROUP DEC2022
GO


---create datafile for partition physical file target
ALTER DATABASE PARTITIONDB
    ADD FILE 
    (
    NAME = NOV2022,
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\PART_01.ndf',
        SIZE = 3072 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 1024 KB
    ) TO FILEGROUP NOV2022

ALTER DATABASE PARTITIONDB
    ADD FILE 
    (
    NAME = DEC2022,
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\PART_02.ndf',
        SIZE = 3072 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 1024 KB
    ) TO FILEGROUP DEC2022

---create partition function
USE PARTITIONDB
GO
CREATE PARTITION FUNCTION [PART_FUNCTION] (datetime)
AS RANGE LEFT FOR VALUES ('20221130','20221231');

---create partition scheme 
CREATE PARTITION SCHEME PART_SCHEME
AS PARTITION [PART_FUNCTION]
TO (NOV2022,DEC2022,[PRIMARY]);


---attach table to partition scheme
CREATE TABLE SAMPLEPARTITION
(
	id int identity,
	tanggal datetime
) ON PART_SCHEME (tanggal)
GO

---create clustered index partition key
CREATE CLUSTERED INDEX IX_ID ON SAMPLEPARTITION (ID,tanggal)
GO

---data sample
INSERT INTO SAMPLEPARTITION (tanggal) VALUES ('2022-11-25 09:29:03.443')
INSERT INTO SAMPLEPARTITION (tanggal) VALUES ('2022-12-30 09:29:03.443')
INSERT INTO SAMPLEPARTITION (tanggal) VALUES ('2023-01-05 09:29:03.443')
INSERT INTO SAMPLEPARTITION (tanggal) VALUES ('2023-02-05 09:29:03.443')
INSERT INTO SAMPLEPARTITION (tanggal) VALUES ('2023-03-05 09:29:03.443')
INSERT INTO SAMPLEPARTITION (tanggal) VALUES ('2023-04-05 09:29:03.443')
INSERT INTO SAMPLEPARTITION (tanggal) VALUES ('2023-05-05 09:29:03.443')
INSERT INTO SAMPLEPARTITION (tanggal) VALUES ('2023-06-05 09:29:03.443')
INSERT INTO SAMPLEPARTITION (tanggal) VALUES ('2023-07-05 09:29:03.443')
INSERT INTO SAMPLEPARTITION (tanggal) VALUES ('2023-08-05 09:29:03.443')



---check ditribution of partitioned data
SELECT DISTINCT o.name as table_name, rv.value as partition_range, fg.name as file_groupName, p.partition_number, p.rows as number_of_rows
FROM sys.partitions p
INNER JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
INNER JOIN sys.objects o ON p.object_id = o.object_id
INNER JOIN sys.system_internals_allocation_units au ON p.partition_id = au.container_id
INNER JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
INNER JOIN sys.partition_functions f ON f.function_id = ps.function_id
INNER JOIN sys.destination_data_spaces dds ON dds.partition_scheme_id = ps.data_space_id AND dds.destination_id = p.partition_number
INNER JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id 
LEFT OUTER JOIN sys.partition_range_values rv ON f.function_id = rv.function_id AND p.partition_number = rv.boundary_id
WHERE o.object_id = OBJECT_ID('SAMPLEPARTITION');



--- Auto partition split
SELECT o.name as table_name, 
  pf.name as PartitionFunction, 
  ps.name as PartitionScheme, 
  MAX(rv.value) AS LastPartitionRange,
  CASE WHEN MAX(rv.value) <= DATEADD(MONTH, 2, GETDATE()) THEN 1 else 0 END AS isRequiredMaintenance
INTO #temp
FROM sys.partitions p
INNER JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
INNER JOIN sys.objects o ON p.object_id = o.object_id
INNER JOIN sys.system_internals_allocation_units au ON p.partition_id = au.container_id
INNER JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
INNER JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
INNER JOIN sys.partition_range_values rv ON pf.function_id = rv.function_id AND p.partition_number = rv.boundary_id
GROUP BY o.name, pf.name, ps.name


SELECT table_name, 
  PartitionFunction, 
  PartitionScheme, 
  LastPartitionRange,
  CONVERT(VARCHAR, DATEADD(MONTH, 1, CAST(LastPartitionRange AS datetime)), 25) AS NewRange,
  'FG_' + CAST(FORMAT(DATEADD(MONTH, 1, CAST(LastPartitionRange AS datetime)),'MM') AS VARCHAR(2)) +
    '_' + 
    CAST(YEAR(DATEADD(MONTH, 1, CAST(LastPartitionRange AS datetime))) AS VARCHAR(4)) AS NewFileGroup,
  'File_'+ CAST(FORMAT(DATEADD(MONTH, 1, CAST(LastPartitionRange AS datetime)),'MM') AS VARCHAR(2)) +
    CAST(YEAR(DATEADD(MONTH, 1, CAST(LastPartitionRange AS datetime))) AS VARCHAR(4)) AS FileName,
  'C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\' AS file_path
--INTO #generateScript
FROM #temp
WHERE isRequiredMaintenance = 1

