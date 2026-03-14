# 🔄 Data Flow

The project follows an ETL-style data pipeline implemented in Microsoft SQL Server, where raw CSV data is ingested, cleaned, standardized, and transformed into an analytical model.
The pipeline follows the Medallion Architecture pattern:
Bronze → Silver → Gold

![Data_flow](docs/data_flow.png)

# 1️⃣ Data Sources

The pipeline begins with **public datasets in CSV** format obtained from the Allegheny County open data portal.
Datasets used from [Allegheny County Tax Liens](https://catalog.data.gov/dataset/allegheny-county-tax-liens-filings-satisfactions-and-current-status):
- **tax_liens_details.csv**
- **tax_liens_summary.csv**

These datasets contain historical records of tax liens filed against properties due to unpaid taxes.
The data includes:

- Property Parcel Identification Number (PIN)
- Municipality and ward information
- Tax fiscal year
- Filing date of the lien
- Transaction amount
- Assigned entity responsible for the lien

These files act as the raw input for the data pipeline.

# 🥉 Bronze Layer — Raw Data Ingestion

The Bronze layer stores the raw data exactly as it was ingested from the CSV files, without applying transformations.
Tables in this layer:

- ```bronze.tax_liens_details```
- ```bronze.tax_liens_summary```

Purpose of the Bronze layer:

- Preserve the original structure of the dataset
- Maintain a reliable source of truth
- Enable data reprocessing if needed

Data ingestion is performed using:

- BULK INSERT
- SQL transactions
- Local CSV datasets

## Conceptual Bronze tables:
![Data Architecture](docs/tables_diagram.png)

Conceptual ingestion flow:
```
CSV Files
   ↓
BULK INSERT
   ↓
bronze.tax_liens_details
bronze.tax_liens_summary
```
## Bronze Layer — Raw Data (```bronze``` schema)
| Schema | Table             | Column            | Data Type     | Description                                               |
| ------ | ----------------- | ----------------- | ------------- | --------------------------------------------------------- |
| bronze | tax_liens_details | _id               | INTEGER       | Unique identifier of the lien record from the raw dataset |
| bronze | tax_liens_details | pin               | VARCHAR(50)   | Property Parcel Identification Number                     |
| bronze | tax_liens_details | block_lot         | VARCHAR(50)   | Cadastral block and lot identifier                        |
| bronze | tax_liens_details | filing_date       | DATE          | Date when the tax lien was filed                          |
| bronze | tax_liens_details | tax_year          | INTEGER       | Fiscal year associated with the tax lien                  |
| bronze | tax_liens_details | dtd               | VARCHAR(50)   | Delinquent Tax Docket number                              |
| bronze | tax_liens_details | lien_description  | VARCHAR(MAX)  | Description of the tax lien                               |
| bronze | tax_liens_details | municipality      | VARCHAR(100)  | Municipality where the property is located                |
| bronze | tax_liens_details | ward              | VARCHAR(10)   | District or ward number                                   |
| bronze | tax_liens_details | last_docket_entry | VARCHAR(50)   | Last recorded legal update in the docket                  |
| bronze | tax_liens_details | amount            | DECIMAL(12,2) | Monetary value associated with the lien transaction       |
| bronze | tax_liens_details | assignee          | VARCHAR(255)  | Entity or individual assigned to the lien                 |

---

| Schema | Table             | Column       | Data Type     | Description                                        |
| ------ | ----------------- | ------------ | ------------- | -------------------------------------------------- |
| bronze | tax_liens_summary | _id          | INTEGER       | Unique identifier of the summary record            |
| bronze | tax_liens_summary | pin          | VARCHAR(50)   | Property Parcel Identification Number              |
| bronze | tax_liens_summary | number       | INTEGER       | Total number of liens associated with the property |
| bronze | tax_liens_summary | total_amount | DECIMAL(15,2) | Total accumulated debt amount for the property     |


---

# 🥈 Silver Layer — Data Cleaning & Standardization
The Silver layer contains cleaned, standardized, and structured data prepared for analysis.

Transformations applied include:

- Column renaming for clarity
- Standardization of naming conventions
- Data type normalization
- Preparation for relational joins

Tables in this layer:
- ```silver.tax_liens_details```
- ```silver.tax_liens_summary```
  
Example of column transformations:

## Silver Layer — Cleaned & Standardized Data (silver schema)

| Schema | Table             | Column             | Data Type     | Description                                         |
| ------ | ----------------- | ------------------ | ------------- | --------------------------------------------------- |
| silver | tax_liens_details | lien_id            | INTEGER       | Unique identifier of the lien record after cleaning |
| silver | tax_liens_details | property_id        | VARCHAR(50)   | Standardized property parcel identifier             |
| silver | tax_liens_details | parcel_location    | VARCHAR(50)   | Property location identifier derived from block/lot |
| silver | tax_liens_details | filing_date        | DATE          | Date when the tax lien was officially recorded      |
| silver | tax_liens_details | tax_fiscal_year    | INTEGER       | Fiscal tax year associated with the lien            |
| silver | tax_liens_details | docket_number      | VARCHAR(50)   | Legal docket number associated with the lien filing |
| silver | tax_liens_details | lien_description   | VARCHAR(MAX)  | Description of the lien or legal filing             |
| silver | tax_liens_details | municipality_name  | VARCHAR(100)  | Municipality where the property is located          |
| silver | tax_liens_details | ward_number        | VARCHAR(10)   | Ward or district identifier                         |
| silver | tax_liens_details | last_legal_update  | VARCHAR(50)   | Last legal status recorded for the lien             |
| silver | tax_liens_details | transaction_amount | DECIMAL(12,2) | Amount related to the lien transaction              |
| silver | tax_liens_details | assigned_entity    | VARCHAR(255)  | Entity responsible for managing the lien            |

---

| Schema | Table             | Column             | Data Type     | Description                                             |
| ------ | ----------------- | ------------------ | ------------- | ------------------------------------------------------- |
| silver | tax_liens_summary | summary_id         | INTEGER       | Unique identifier of the summary record                 |
| silver | tax_liens_summary | property_id        | VARCHAR(50)   | Property identifier linking summary and detail tables   |
| silver | tax_liens_summary | active_liens_count | INTEGER       | Number of active tax liens associated with the property |
| silver | tax_liens_summary | total_debt_amount  | DECIMAL(15,2) | Total outstanding debt amount for the property          |

---

# 🥇 Gold Layer — Analytical Model

The Gold layer contains the analytics-ready dataset used for reporting and data analysis.
In this project, the Gold layer is implemented as the view:
```gold.fact_tax_liens```
This analytical view:
- Combines datasets from the Silver layer
- Computes analytical metrics
- Provides a dataset optimized for queries and reporting

## Gold Layer — Analytical Model (gold schema)

| Schema | Table/View     | Column             | Data Type     | Description                                            |
| ------ | -------------- | ------------------ | ------------- | ------------------------------------------------------ |
| gold   | fact_tax_liens | lien_id            | BIGINT        | Generated sequential identifier for analytical queries |
| gold   | fact_tax_liens | property_id        | VARCHAR(50)   | Property parcel identifier                             |
| gold   | fact_tax_liens | parcel_location    | VARCHAR(50)   | Location identifier derived from cadastral data        |
| gold   | fact_tax_liens | filing_date        | DATE          | Date when the lien was filed                           |
| gold   | fact_tax_liens | tax_fiscal_year    | INTEGER       | Fiscal year associated with the lien                   |
| gold   | fact_tax_liens | docket_number      | VARCHAR(50)   | Legal docket identifier                                |
| gold   | fact_tax_liens | lien_description   | VARCHAR(MAX)  | Description of the lien                                |
| gold   | fact_tax_liens | municipality_name  | VARCHAR(100)  | Municipality where the property is located             |
| gold   | fact_tax_liens | ward_number        | VARCHAR(10)   | Ward or district identifier                            |
| gold   | fact_tax_liens | last_legal_update  | VARCHAR(50)   | Last legal status update                               |
| gold   | fact_tax_liens | assigned_entity    | VARCHAR(255)  | Entity assigned to the lien                            |
| gold   | fact_tax_liens | transaction_amount | DECIMAL(12,2) | Amount of the individual lien transaction              |
| gold   | fact_tax_liens | total_debt_amount  | DECIMAL(15,2) | Total accumulated lien debt for the property           |
| gold   | fact_tax_liens | active_liens_count | INTEGER       | Total number of liens associated with the property     |

---

# 📊 Final Pipeline Output

The pipeline transforms raw datasets into a structured analytical model ready for exploration and analysis.
Complete pipeline flow:

```
CSV DATASETS
     ↓
Bronze Layer
(raw ingestion)
     ↓
Silver Layer
(cleaned & standardized)
     ↓
Gold Layer
(analytical model)
```

The final dataset enables analysis such as:
- Properties with the highest outstanding tax debt
- Municipalities with the highest lien activity
- Distribution of tax debt across properties
- Trends in tax lien filings over time
