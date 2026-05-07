# Hospital Patient Flow Analytics

## Project Overview

This project demonstrates a real-time data engineering pipeline for healthcare, designed to monitor hospital capacity and patient flow across departments using Azure cloud services.
The pipeline ingests streaming data via Apache Kafka (Azure Event Hubs), processes it through Databricks (PySpark) across Bronze → Silver → Gold layers, and stores analytics-ready data in Azure Synapse SQL Pool for visualization in Power BI.

### Part 1 – Data Engineering
Build the real-time ingestion and transformation pipeline.
### Part 2 – Analytics
Connect Synapse to Power BI and design an interactive dashboard for hospital KPIs.

-----

# Pipeline / Architecture

![image alt](https://github.com/vinaybommireddy/hospital-patient-flow-analytics/blob/9f2b00ebef1aac17404964b96a2a56b3ebd8f677/Architecture.png)

The pipeline covers: 

- Data Source 
- Kafka Streaming 
- Data Lake (Bronze/Silver/Gold) 
- Synapse SQL Pool 
- Power BI Analytics, secured by Azure Key Vault and Azure Active Directory.

---

# Star Schema Design

The Gold Layer in Synapse SQL Pool follows a star schema optimized for analytics workloads.

## Fact Table

### FactPatientFlow — patient visits, admission timestamps, wait times, discharge status, bed occupancy
Dimension Tables:

    DimDepartment — Department name, type, and capacity
    DimPatient — Patient demographics (age, gender)
    DimTime — Date and time attributes for trend analysis
---

# Implementation

1. Security Setup (Key Vault & Azure AD)
2. Event Hub (Kafka) Setup
3. Data Simulation
4. Storage Setup (Data Lake)
5. Databricks Processing
6. Synapse SQL Pool

---
# Data Analytics & Dashboard

## Synapse - Power BI Connection
- Connected Azure Synapse SQL Pool to Power BI using a direct SQL endpoint.
- Imported FactPatientFlow and all Dimension tables.
- Established Star Schema relationships for optimized reporting.

![image alt](https://github.com/vinaybommireddy/hospital-patient-flow-analytics/blob/e949b773e882c372123169d84d05662a39aeff59/Hospital_data.png)

#Key Outcomes

-End-to-End Real-Time Pipeline: From Kafka ingestion → Databricks transformation → Synapse warehouse → Power BI analytics.
-Scalable Medallion Architecture: Bronze/Silver/Gold layers adaptable to any hospital dataset size.
-Enterprise-Grade Security: Key Vault and Azure AD integration for production-ready access control.
-Actionable Business Insights: Hospital admins can monitor bed usage, patient flow, and department efficiency in real time.
-Portfolio Depth: Demonstrates both Data Engineering and Analytics capabilities end-to-end.
