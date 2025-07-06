# 🗃️ DBT Crime Data Project

This project demonstrates dbt's ability to structure, transform, and automate data pipelines using real-world public data. It leverages the Socrata Open Data API to collect crime data from major U.S. cities and organizes it into clean, business-ready tables.

## 📦 Project Overview

This dbt project pulls crime data for the current year from the Socrata Open Data API for:

- San Francisco  
- Chicago  
- Seattle  

The data flows through a structured transformation pipeline using raw, staging, and mart schemas to ensure clean, reliable reporting.

## ⚙️ Features

✔ Raw, Staging, and Mart layer separation following dbt best practices  
✔ Automated data pipeline scheduled to run every **Monday, Wednesday, and Friday**  
✔ Real-world crime datasets from open city sources via Socrata API  
✔ Scalable structure to support additional cities or metrics  

## 🗄️ Database Structure

├─ raw/ -- Contains untransformed ingested API data
├─ staging/ -- Cleans and standardizes raw data for analysis
├─ marts/ -- Final, analytics-ready tables for reporting

## 🔗 Data Sources

- [San Francisco Crime Data - Socrata Open Data API](https://data.sfgov.org/Public-Safety/Police-Department-Incident-Reports-2018-to-Present/wg3w-h783)  
- [Chicago Crime Data - Socrata Open Data API](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2)  
- [Seattle Crime Data - Socrata Open Data API](https://data.seattle.gov/Public-Safety/Crime-Data/4fs7-3vj5)  

## ⏰ Scheduling

The project includes a scheduled job that automatically triggers the dbt pipeline every **Monday, Wednesday, and Friday**, ensuring fresh data for analysis.

## 🚀 Getting Started

**Prerequisites:**  
- Python and dbt installed  
- Access to your target data warehouse (e.g., BigQuery, Snowflake, etc.)  

**Basic Setup:**  
```bash
git clone https://github.com/MatthewPBriones/DBTPractice.git  
cd DBTPractice  
dbt deps  
dbt seed  
dbt run  
dbt test  
