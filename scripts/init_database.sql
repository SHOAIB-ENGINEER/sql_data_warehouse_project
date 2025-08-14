/* 
=============================================================
Create Database and Schemas
=============================================================
Scirpt Purpose :
  This script creates a new database named 'DataWarehouse' after creating it already exists.
If the database exists, it is droped and recreated. Additionally, the script sets up three schemas 
within the database :  'Bronze', 'Silver', and 'Gold'.

WARNING :
 Running this script will drop the entire 'datawarehouse' database if it exists.
 All data in the database will de permanently deleted. Proceed with caution
 and ensure that you have prpper backup before running this script.
*/

use master;
go

--drop and recreate the 'datawarehouse' database
  IF EXISTS (SELECT  1 FROM sys.database WHERE name = 'Datawarehouse')
BEGIN
      ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
      DROP DATABASE Datawarehouse;
END;
GO

---CREATE THE 'DATAWAREHOUSE' DATABASE
CREATE DATABASE Datawarehouse;
go

use Datawarehouse;
go

--- Create Schemas
Create Schema bronze;
go

create schema silver;
go

create schema gold;
go
