/*
================================================================================
              CREATE DATABASE 'AlleghenyTaxLiens' AND brozce TABLES
================================================================================

[!] DESCRIPTION:
    This script automates the creation of the 'AlleghenyTaxLiens' database and 
    its core tables used to store property tax lien records from Allegheny County.

    It includes the creation of two tables:
        - tax_liens_details
        - tax_liens_summary

    The script also performs bulk data ingestion from CSV datasets using
    BULK INSERT to efficiently load large volumes of records into SQL Server.

[!] DATASET OVERVIEW:
    The dataset contains public records related to property tax liens including:

        • Property identifier (PIN)
        • Parcel block/lot location
        • Filing date and tax year
        • Lien descriptions
        • Municipality and ward information
        • Legal docket updates
        • Individual lien amounts
        • Assigned entities
        • Summary of total liens per property

[!] STRUCTURE:
    1. Verify if the database already exists.
    2. If it exists, switch to SINGLE_USER mode and drop it safely.
    3. Create the 'AlleghenyTaxLiens' database.
    4. Change context to the new database.
    5. Create the tables:
            - bronze.tax_liens_details
            - bronze.tax_liens_summary
			- silver.tax_liens_details
            - silver.tax_liens_summary
    6. Truncate existing data before import.
    7. Load CSV datasets using BULK INSERT within transactions.
    8. Log execution time and handle errors with TRY/CATCH blocks.

[X] WARNING:
    - SQL Server requires READ permissions on the dataset directory.
    - Ensure that the CSV files are not open in Excel or another program
      during execution.
    - File paths must exist on the same machine where SQL Server is running.

[+] DATA SOURCE:
    Allegheny County Property Tax Liens Public Dataset.
	https://catalog.data.gov/dataset/allegheny-county-tax-liens-filings-satisfactions-and-current-status

================================================================================
*/

USE master;
GO 

-- =========================================================
--			  CREATE DATA BASE AlleghenyTaxLiens
-- =========================================================
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'AlleghenyTaxLiens')
BEGIN
	PRINT '[!] Deleting existing AlleghenyTaxLiens database.'
	ALTER DATABASE AlleghenyTaxLiens SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	DROP DATABASE AlleghenyTaxLiens
END;
GO

PRINT '[+] Creating AlleghenyTaxLiens database.'
CREATE DATABASE AlleghenyTaxLiens
GO

USE AlleghenyTaxLiens
GO

-- =========================================================
--			 CREATE AlleghenyTaxLiens's SCHEMAS
-- =========================================================

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
	PRINT '[+] Creating bronze Schema.'
    EXEC('CREATE SCHEMA bronze AUTHORIZATION dbo');
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
	PRINT '[+] Creating silver Schema.'
    EXEC('CREATE SCHEMA silver AUTHORIZATION dbo');
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
	PRINT '[+] Creating gold Schema.'
    EXEC('CREATE SCHEMA gold AUTHORIZATION dbo');
END
GO

-- =========================================================
--	CREATE TABLES "tax_liens_details" & "tax_liens_summary"
-- =========================================================

IF OBJECT_ID(N'bronze.tax_liens_details', N'U') IS NULL
BEGIN 
	-- Create "bronze.tax_liens_details" table
	BEGIN TRY
		BEGIN TRANSACTION;
			CREATE TABLE [bronze].[tax_liens_details] (
				_id				  INTEGER PRIMARY KEY,
				pin				  VARCHAR(50),      -- Identificador de la propiedad
				block_lot		  VARCHAR(50),      -- Ubicación catastral
				filing_date	      DATE,            -- Fecha de registro
				tax_year		  INTEGER,         -- Año fiscal del impuesto
				dtd				  VARCHAR(50),     -- Número de documento/expediente
				lien_description  VARCHAR(MAX),    -- Descripción del gravamen
				municipality	  VARCHAR(100),    -- Municipio
				ward		      VARCHAR(10),     -- Distrito/Barrio
				last_docket_entry VARCHAR(50),     -- Última actualización legal
				amount		      DECIMAL(12, 2),  -- Monto del trámite específico
				assignee		  VARCHAR(255)     -- Persona o entidad asignada
			);
		COMMIT TRANSACTION;
		PRINT '[+] bronze.tax_liens_details created successfully';
	END TRY	
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        PRINT '[x] Err creating bronze.tax_liens_details: ' + ERROR_MESSAGE();
	END CATCH
END
ELSE 
BEGIN
	PRINT '[!] Table bronze.tax_liens_details already exists.';
END

GO

IF OBJECT_ID(N'bronze.tax_liens_summary', N'U') IS NULL
BEGIN 
	-- Create "bronze.tax_liens_summary" table
	BEGIN TRY
		BEGIN TRANSACTION;
			CREATE TABLE [bronze].[tax_liens_summary] (
				_id			 INTEGER PRIMARY KEY,
				pin			 VARCHAR(50),           -- Identificador (Relacionado con filings)
				number		 INTEGER,               -- Cantidad de gravámenes activos (probablemente)
				total_amount DECIMAL(15, 2)         -- Suma total de deuda
			);
		COMMIT TRANSACTION;
		PRINT '[+] bronze.tax_liens_summary created successfully'
	END TRY	
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        PRINT '[x] Err creating bronze.tax_liens_summary: ' + ERROR_MESSAGE();
	END CATCH
END
ELSE 
BEGIN
	PRINT '[!] Table bronze.tax_liens_summary already exists.';
END

GO

IF OBJECT_ID(N'silver.tax_liens_details', N'U') IS NULL
BEGIN 
	-- Create "silver.tax_liens_details" table
	BEGIN TRY
		BEGIN TRANSACTION;
			CREATE TABLE [silver].[tax_liens_details] (
				lien_id              INTEGER PRIMARY KEY,       -- Antes: _id
				property_id          VARCHAR(50),               -- Antes: pin (Parcel ID Number)
				parcel_location      VARCHAR(50),               -- Antes: block_lot
				filing_date          DATE,                      -- Se mantiene por claridad
				tax_fiscal_year      INTEGER,                   -- Antes: tax_year
				docket_number        VARCHAR(50),               -- Antes: dtd (Delinquent Tax Docket)
				lien_description     VARCHAR(MAX),              -- Se mantiene
				municipality_name    VARCHAR(100),              -- Antes: municipality
				ward_number          VARCHAR(10),               -- Antes: ward
				last_legal_update    VARCHAR(50),               -- Antes: last_docket_entry
				transaction_amount   DECIMAL(12, 2),            -- Antes: amount
				assigned_entity      VARCHAR(255)               -- Antes: assignee
			);
		COMMIT TRANSACTION;
		PRINT '[+] silver.tax_liens_details created successfully'
	END TRY	
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        PRINT '[x] Err creating silver.tax_liens_details: ' + ERROR_MESSAGE();
	END CATCH
END
ELSE 
BEGIN
	PRINT '[!] Table silver.tax_liens_details already exists.';
END

GO

IF OBJECT_ID(N'silver.tax_liens_summary', N'U') IS NULL
BEGIN 
	-- Create "silver.tax_liens_summary" table
	BEGIN TRY
		BEGIN TRANSACTION;
			CREATE TABLE [silver].[tax_liens_summary] (
				summary_id           INTEGER PRIMARY KEY,       -- Antes: _id
				property_id          VARCHAR(50),               -- Antes: pin (Consistente con la tabla detalles)
				active_liens_count   INTEGER,                   -- Antes: number (Especifica que es un conteo)
				total_debt_amount    DECIMAL(15, 2)             -- Antes: total_amount (Más descriptivo para finanzas)
			);
		COMMIT TRANSACTION;
		PRINT '[+] silver.tax_liens_summary created successfully'
	END TRY	
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        PRINT '[x] Err creating silver.tax_liens_summary: ' + ERROR_MESSAGE();
	END CATCH
END
ELSE 
BEGIN
	PRINT '[!] Table silver.tax_liens_summary already exists.';
END

GO

-- =========================================================
--	 INSERT INTO "tax_liens_details" & "tax_liens_summary"
-- =========================================================

BEGIN TRY
	PRINT '[...] Loading tax_liens_details...';
	DECLARE @StartTime1 DATETIME2 = SYSDATETIME();
	BEGIN TRANSACTION;
		TRUNCATE TABLE [bronze].[tax_liens_details];

		BULK INSERT [bronze].[tax_liens_details]
		FROM 'D:\Data\tax_liens_details.csv'			-- CHANGE THIS
		WITH (
			FORMAT = 'CSV',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0A',
			KEEPNULLS,
			CODEPAGE = '65001',
			TABLOCK
		);
	COMMIT TRANSACTION;
	DECLARE @EndTime1 DATETIME2 = SYSDATETIME();

	PRINT '[+] Details exec time: ' + CAST(DATEDIFF(SECOND, @StartTime1, @EndTime1) AS VARCHAR(10)) + ' sec.';
	PRINT '[+] Data successfully inserted in tax_liens_details.';
END TRY
BEGIN CATCH
	PRINT '[x] Err to import tax_liens_details: ' + ERROR_MESSAGE();
END CATCH

BEGIN TRY
	PRINT '[...] Loading tax_liens_summary...';
	DECLARE @StartTime2 DATETIME2 = SYSDATETIME();
	BEGIN TRANSACTION;
		TRUNCATE TABLE [bronze].[tax_liens_summary];

		BULK INSERT [bronze].[tax_liens_summary]
		FROM 'D:\Data\tax_liens_summary.csv'			-- CHANGE THIS
		WITH (
			FORMAT = 'CSV',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0A',
			KEEPNULLS,
			CODEPAGE = '65001',
			TABLOCK
		);
	COMMIT TRANSACTION;
	DECLARE @EndTime2 DATETIME2 = SYSDATETIME();

	PRINT '[+] Details exec time: ' + CAST(DATEDIFF(SECOND, @StartTime2, @EndTime2) AS VARCHAR(10)) + ' sec.';
	PRINT '[+] Data successfully inserted in tax_liens_summary.';
END TRY
BEGIN CATCH
	PRINT '[x] Err to import tax_liens_summary: ' + ERROR_MESSAGE();
END CATCH