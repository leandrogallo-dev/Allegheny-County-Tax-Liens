/*
================================================================================
          ETL PROCESS: BRONZE TO SILVER - AlleghenyTaxLiens
================================================================================

[!] DESCRIPTION:
    This script automates the Data Transformation (ETL) process from the 
    'Bronze' (raw) layer to the 'Silver' (curated) and 'Gold' (analytics) layers.
    
    The process handles:
        - Data cleaning and normalization.
        - Null value handling for assigned entities.
        - Advanced analytics using Window Functions (SUM, COUNT, ROW_NUMBER).
        - Automated View creation for the final reporting layer.

[!] TRANSFORMATIONS:
    • silver.tax_liens_details:
        - Filters out inconsistent historical records (tax_year > 1900).
        - Maps technical fields (pin, dtd, amount) to business terms.
        - Business Logic: Replaces NULLs with 'Unknown' for assigned_entity.
        
    • silver.tax_liens_summary:
        - Ingests aggregated property-level debt metrics.

    • gold.fact_tax_liens (VIEW):
        - Join between details and summary.
        - Calculates running totals (total_debt_amount) per property.
        - Generates unique lien IDs using ROW_NUMBER partition.

[!] STRUCTURE:
    1. TRY/CATCH Block: Load silver.tax_liens_details (TRUNCATE + INSERT).
    2. TRY/CATCH Block: Load silver.tax_liens_summary (TRUNCATE + INSERT).
    3. VIEW Creation:   Deploy gold.fact_tax_liens for BI/Reporting.
    4. LOGGING:         Captures execution time and error messages per block.

[X] WARNING:
    - This script uses TRUNCATE; existing data in Silver tables will be lost.
    - View creation requires the existence of both Silver tables.

[+] LAYERS:
    Source:  [bronze].[tax_liens_details | tax_liens_summary]
    Target:  [silver].[tax_liens_details | tax_liens_summary]

================================================================================
*/

BEGIN TRY
	PRINT '[...] Loading silver.tax_liens_details...';
	DECLARE @StartTime1 DATETIME2 = SYSDATETIME();
	TRUNCATE TABLE silver.tax_liens_details;
	INSERT INTO silver.tax_liens_details (
		lien_id,			 property_id,        
		parcel_location,	 filing_date,        
		tax_fiscal_year,	 docket_number,		
		lien_description,    municipality_name,	
		ward_number,		 last_legal_update,  
		transaction_amount,	 assigned_entity		
	)
	SELECT 
		tld._id					   AS lien_id,
		tld.pin					   AS property_id,
		tld.block_lot			   AS parcel_location,
		tld.filing_date			   AS filing_date,
		tld.tax_year			   AS tax_fiscal_year,
		tld.dtd					   AS docket_number,
		tld.lien_description	   AS lien_description,
		tld.municipality		   AS municipality_name,
		tld.ward				   AS ward_number,
		tld.last_docket_entry	   AS last_legal_update,
		tld.amount				   AS transaction_amount,
		CASE 
			WHEN tld.assignee  IS NULL THEN 'Unknown'
			ELSE tld.assignee
		END						   AS assigned_entity
	FROM bronze.tax_liens_details  AS tld
	WHERE tld.tax_year > 1900;

	DECLARE @EndTime1 DATETIME2 = SYSDATETIME();
	PRINT '[+] Details exec time: ' + CAST(DATEDIFF(SECOND, @StartTime1, @EndTime1) AS VARCHAR(10)) + ' sec.';
	PRINT '[+] Data successfully inserted in silver.tax_liens_details.';
END TRY
BEGIN CATCH
	PRINT '[x] Err to import silver.tax_liens_details: ' + ERROR_MESSAGE();
END CATCH

GO

BEGIN TRY
	DECLARE @StartTime2 DATETIME2 = SYSDATETIME();
	PRINT '[...] Loading silver.tax_liens_summary...';
	TRUNCATE TABLE silver.tax_liens_summary;
	INSERT INTO silver.tax_liens_summary (
		summary_id,        		
		property_id,       
		active_liens_count,
		total_debt_amount
	)
	SELECT 
		tls._id					  AS summary_id,
		tls.pin					  AS property_id,
		tls.number				  AS active_liens_count,
		tls.total_amount		  AS total_debt_amount
	FROM bronze.tax_liens_summary AS tls;
	DECLARE @EndTime2 DATETIME2 = SYSDATETIME();
	PRINT '[+] Details exec time: ' + CAST(DATEDIFF(SECOND, @StartTime2, @EndTime2) AS VARCHAR(10)) + ' sec.';
	PRINT '[+] Data successfully inserted in silver.tax_liens_summary.';
END TRY
BEGIN CATCH
	PRINT '[x] Err to import silver.tax_liens_summary: ' + ERROR_MESSAGE();
END CATCH