# Sales-Data-Analytics-End-to-End-Azure-Data-Engineering-Project
![On Premise SSMS](https://github.com/user-attachments/assets/59c0ef3f-7785-47ac-877d-42e4c83f9b69)
Here's the updated **README** file with the inclusion of **Azure Key Vault** for storing on-premises SSMS credentials:  

---

# **Sales Data Analytics: End-to-End Azure Data Engineering Project**  

## **Overview**  
This project demonstrates the implementation of an end-to-end data engineering pipeline using Azure services to process and analyze sales data. The architecture follows the **Medallion Architecture**, utilizing **Azure Data Factory**, **Azure Databricks**, **Delta Lake**, **Unity Catalog**, and **Azure Key Vault** to build a scalable, reliable, and secure data platform.  

The pipeline ingests data from an **On-premises SQL Server (SSMS)**, processes and transforms it using PySpark in Databricks, and stores it in Delta Lake following the **Bronze, Silver, and Gold layer architecture**. The enriched data is then visualized using **Power BI**.  

---

## **Architecture**  

### **Medallion Architecture**
1. **Bronze Layer**  
   - Raw data ingestion from **On-premises SQL Server (SSMS)** using **Azure Data Factory (ADF)** and a **self-hosted Integration Runtime (IR)**.  
   - Data stored in **Azure Data Lake Storage Gen2 (ADLS)** as the entry point for raw data.  

2. **Silver Layer**  
   - Processed and cleaned data using **PySpark** in **Azure Databricks**.  
   - Applied transformations, filtering, and joins to create curated datasets.  
   - Stored as Delta Lake tables for efficient query performance.  

3. **Gold Layer**  
   - Aggregated and enriched data, ready for analytics and reporting.  
   - Data used for querying in **Databricks SQL Warehouse** and visualization in **Power BI**.  

---

## **Key Components**  

### **Azure Services Used**  
- **Azure Data Factory (ADF):**  
  - For data ingestion from **On-premises SSMS**.  
  - Orchestrates pipelines to ingest, transform, and load data.  

- **Azure Databricks:**  
  - Data cleaning, transformations, and processing using **PySpark**.  
  - Utilized **Delta Lake** for storing data in Bronze, Silver, and Gold layers.  

- **Delta Lake:**  
  - Provides ACID transaction support and scalable data storage.  
  - Used for **incremental data loads** and **Type 1 Slowly Changing Dimensions (SCD1)** implementation.  

- **Unity Catalog:**  
  - Ensures data governance and security.  
  - Implements catalog-level and schema-level access controls.  

- **Azure Key Vault:**  
  - Used to securely store **on-premises SQL Server credentials**.  
  - Integrated with **ADF managed identity** for seamless authentication.  

### **Data Transformation and Governance**  
- **Full and Incremental Data Loads:**  
  - Supported **UPSERT** operations with Delta Lake.  
  - Efficient handling of incremental data with watermarking.  

- **Type 1 SCD Implementation:**  
  - Tracks updates to data without maintaining historical versions.  

### **Visualization**  
- **Power BI:**  
  - Connected to the **Gold Layer** tables for visualizing enriched sales data.  
  - Enabled actionable insights through dashboards and reports.  

---

## **Implementation Steps**  

### 1. **Data Ingestion**  
   - **Source:** On-premises SQL Server (SSMS).  
   - **Ingestion Tool:** Azure Data Factory (ADF) with self-hosted IR.  
   - **Data Storage:** Raw data stored in the Bronze layer of Delta Lake.  
   - **Credential Management:** SSMS credentials securely stored in **Azure Key Vault** and accessed by ADF via managed identity.  

### 2. **Data Transformation**  
   - Data processed in Databricks using PySpark:  
     - Cleaned, filtered, and joined datasets.  
     - Incremental load implemented using Delta Lake features.  

   - **Unity Catalog** used for managing metadata and enforcing data governance.  

### 3. **Data Storage Layers**  
   - **Bronze Layer:** Raw data.  
   - **Silver Layer:** Cleaned and transformed data with SCD1 implementation.  
   - **Gold Layer:** Aggregated data for reporting and analytics.  

### 4. **Visualization**  
   - Connected **Gold Layer** tables to Power BI for creating dashboards.  
   - Enabled real-time and historical analysis of sales data.  

---

## **Key Features**  
- **Scalable Architecture:** Designed for handling large-scale data.  
- **Secure Credential Management:** Credentials stored in **Azure Key Vault** to enhance security.  
- **Data Governance:** Implemented using Unity Catalog in Databricks.  
- **Incremental Data Processing:** Efficient processing of data updates.  
- **End-to-End Integration:** From data ingestion to reporting using Azure services.  

---

## **Screenshots**  

### **1. Azure Data Factory Pipeline**  
<img width="959" alt="image" src="https://github.com/user-attachments/assets/7b2d45a1-20b7-4290-bf9f-09bfd8d3e11f" />


### **2. Databricks Notebooks**  
<img width="959" alt="image" src="https://github.com/user-attachments/assets/7ac09862-f3c1-47cc-8e01-5d779ee2bedb" />


### **3. Azure Data Lake Gen2**  
<img width="960" alt="image" src="https://github.com/user-attachments/assets/2c112f18-75e1-4bdd-8ca7-b76f41ea267f" />


---

## **Prerequisites**  
- Azure Subscription with access to the following services:  
  - Azure Data Factory  
  - Azure Databricks  
  - Azure Data Lake Storage Gen2  
  - Azure Key Vault  
  - Power BI  
- On-premises SQL Server with sales data.  
- Self-hosted Integration Runtime configured in ADF.  

---

## **How to Run the Project**  

### **Step 1: Data Ingestion**  
1. Configure ADF to connect to the on-premises SQL Server using self-hosted IR.  
2. Store SSMS credentials in Azure Key Vault.  
3. Grant ADF managed identity the required access to Key Vault for retrieving credentials.  
4. Create pipelines to copy raw data into the Bronze layer.  

### **Step 2: Data Processing**  
1. Use Databricks to process and transform data.  
2. Implement PySpark scripts for filtering, joining, and incremental loading.  

### **Step 3: Data Visualization**  
1. Connect Power BI to the Gold layer tables in Databricks Catalog Tables.  
2. Build dashboards for insights and reporting.  

---

