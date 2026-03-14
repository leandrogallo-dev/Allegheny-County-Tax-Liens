/*
================================================================================
                    GOLD LAYER: DATA CONSUMPTION (VIEW)
================================================================================
*/

PRINT '[...] Creating View gold.fact_tax_liens...';

IF OBJECT_ID('gold.fact_tax_liens', 'V') IS NOT NULL
    DROP VIEW gold.fact_tax_liens;
GO

CREATE VIEW gold.fact_tax_liens AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY tld.property_id, tld.tax_fiscal_year) AS lien_id,
    tld.property_id,
    tld.parcel_location,
    tld.filing_date,
    tld.tax_fiscal_year,		
    tld.docket_number,
    tld.lien_description,
    tld.municipality_name,
    tld.ward_number,
    tld.last_legal_update,
    tld.assigned_entity,
    tld.transaction_amount,
    SUM(tld.transaction_amount) 
        OVER (PARTITION BY tld.property_id) AS total_debt_amount,
    COUNT(*) 
        OVER (PARTITION BY tld.property_id) AS active_liens_count
FROM silver.tax_liens_details AS tld
INNER JOIN silver.tax_liens_summary AS tls
    ON tld.property_id = tls.property_id
WHERE tls.total_debt_amount IS NOT NULL
      AND tld.filing_date > '2013-07-03';

GO

PRINT '[+] View gold.fact_tax_liens created successfully.';