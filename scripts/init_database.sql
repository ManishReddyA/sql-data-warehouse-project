/*
script purpose:
  creating a datawarehouse database and added three schemas bronze, silver, and gold into the database.
*/
USE master;
create database DataWarehouse;
go
USE DataWarehouse;
go
create schema bronze;
go
create schema silver;
go
create schema gold;
