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

