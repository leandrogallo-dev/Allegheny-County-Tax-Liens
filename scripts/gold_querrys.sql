-- Total de deuda por municipio
SELECT 
	municipality_name, 
	COUNT(*) AS total_liens,
	SUM(transaction_amount) AS total_debt
FROM gold.fact_tax_liens
GROUP BY municipality_name
ORDER BY total_debt DESC;

-- Top 10 propiedades con mayor deuda
SELECT TOP 10
	property_id,
	transaction_amount
FROM gold.fact_tax_liens
ORDER BY transaction_amount DESC;

-- Deuda total por año fiscal
SELECT 
	tax_fiscal_year,
	SUM(transaction_amount) AS total_year_amount
FROM gold.fact_tax_liens
GROUP BY tax_fiscal_year
ORDER BY tax_fiscal_year;

-- Promedio de deuda por gravamen
SELECT 
    ROUND(AVG(transaction_amount), 2) AS avg_lien_amount,
    MIN(transaction_amount) AS min_lien,
    MAX(transaction_amount) AS max_lien
FROM gold.fact_tax_liens

-- Municipios con más gravámenes
SELECT TOP 10
    municipality_name,
    COUNT(*) AS total_liens
FROM gold.fact_tax_liens
GROUP BY municipality_name
ORDER BY total_liens DESC;

-- Cantidad de gravámenes por descripción
SELECT 
	lien_description,
	COUNT(*) AS total_cases
FROM gold.fact_tax_liens
GROUP BY lien_description
ORDER BY total_cases DESC;

-- Municipios con mayor deuda promedio
SELECT 
	municipality_name,
	AVG(transaction_amount) AS avg_debt
FROM gold.fact_tax_liens
GROUP BY municipality_name
ORDER BY avg_debt DESC;

-- Join entre tablas (consulta más profesional)
SELECT 
	*
FROM gold.fact_tax_liens
ORDER BY property_id, filing_date;

-- Top entidades asignadas
SELECT TOP 10
	assigned_entity,
	COUNT(*) AS total_liens
FROM gold.fact_tax_liens
WHERE assigned_entity IS NOT NULL
GROUP BY assigned_entity
ORDER BY total_liens DESC

-- Evolución de gravámenes por año
SELECT 
	YEAR(filing_date) AS filing_year,
	COUNT(*) AS liens_filed
FROM gold.fact_tax_liens
GROUP BY YEAR(filing_date)
ORDER BY liens_filed DESC

-- Calculo bien hecho de total_debt_amount y active_liens_count por property_id
SELECT property_id, SUM(transaction_amount) AS ta, total_debt_amount, COUNT(*) AS ae, active_liens_count
FROM gold.fact_tax_liens
GROUP BY property_id, total_debt_amount, active_liens_count
HAVING NOT(SUM(transaction_amount) = total_debt_amount AND COUNT(*) = active_liens_count) -- anything row = good