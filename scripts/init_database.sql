/*
=================================================================================
Create database and schemas
=================================================================================
Script purpose:
  This script creates a new database called 'DataWarehouse'. If the database exists, it will show and error indicating it
  already exists. Additionally, the script sets up the schemas namely 'bronze', 'silver' and 'gold'.
*/

-- Create Database 'DataWarehouse'
CREATE DATABASE DataWarehouse;

-- Create a schema for each layer
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
