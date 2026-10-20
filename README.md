<img width="81" height="17" alt="image" src="https://github.com/user-attachments/assets/0ba2f6b1-7f92-41c9-a8b6-76c09c3464c1" /><img width="101" height="17" alt="image" src="https://github.com/user-attachments/assets/b86f9960-548c-4433-b22c-b672f88b5286" />
# Retail Data Engineering & Analytics Platform

## 1. Project Overview
The goal of this project is to build an end-to-end data engineering and analytics platform for a retail business. The platform is designed to ingest data from multiple sources, store and process it, and deliver analytical reports based on business stakeholders' requirements.

## 2. Business Scenario (Simulated)
I work for a consulting company, and I was asked to build a data platform for a retail client. The client has historical sales data, receives new sales transactions daily, and also has customer and product data. They need a centralized platform to process this data and support historical and near-real-time sales analytics. The retail company have a local system where they used Excel to analyse the data, and now they want to have their solution on Fabric platform. 


### Stakeholders

| Stakeholder | Role | Business Needs |
|---|---|---|
| **Retail Business Owner** | Understand overall business performance | Define the business requirements and the information needed from the platform|
| **Retail Sales Manager**  | Monitor sales, revenue, profit, and product performance | Use Power BI reports to track total revenue, total profit, total units sold, and profit margin, with filters for year, region, and  real-time sales |
|**Business Analyst** | Explore sales data and investigate business trends |Use the sales reports to analyze trends and investigate why sales increase or decrease over time |
| **Data Engineer** | Build and maintain the data platform | Develop pipelines to ingest and clean daily sales data, load it into the warehouse, and process real-time sales events|


### Company Background

The client is a retail business that sells different types of products to customers from different regions. The business keeps records of its customers, products, orders, and sales transactions. The company generates new sales data daily and also has historical sales data to track performance over time.



### Business Problem
The retail company already has its data stored on a local server and uses Excel to analyze it. However, the company now wants to move to Microsoft Fabric. They need a solution that can process daily batches, update existing tables, and monitor sales in near real time. Working with Excel makes it difficult to keep the reports updated as new sales data arrives every day.

## 3. Data Model

### Source Tables
The retail business provides five source tables containing customer, product, and sales data. These tables are used as the input for the data platform.
| Table | Description |Load Type |
|---|---|---|
| **Customers** | Stores customer information, such as customer ID, region, city, age, and signup date | Full Load|
| **Products**  | Contains product information, including product ID, name, and category  | Full Load |
| **Categories** |Stores the available product categories | Full Load |
| **Orders**  | Contains general order information, such as order ID, customer ID, and order date  | Full Load + Daily Incremental |
|**Order_details** | Contains the products purchased in each order, including quantity, price, discounts, and return details | Full Load + Daily Incremental |


### Source Data ERD
The diagram below shows the tables and their relationships.
![Data Model ERD Diagram](docs/diagrams/data%20model%20ERD%20diagram.jpeg)

| Relationship | Explanation | 
|---|---|
| **One Customer -> Many Orders** | One customer can place multiple orders, but each order belongs to one customer |
| **One Order -> Many Order_Details** | One order can have many Order_Details, as it contains detailed information about this order. For example, one order can have different products, so order details give us information about the order and what it contains |
| **One Product -> Many Order_Details** | One product can appear in multiple order details, but each order detail refers to one product |
| **One Category -> Many Products** | One category can contain multiple products, but each product belongs to one category |


## 4. Business Requirements

### Technical Scope & Requirements
- **Historical Data Processing:** The company wants to migrate its existing historical customer, product, and sales data to Fabric Lakehouse. The data should be cleaned, organized, and made available for reporting without losing existing records.
- **Daily Incremental Ingestion:** The platform should process daily batches of orders and order details and load them into the Fabric Lakehouse. It should handle both new and updated records without creating duplicates. The pipeline should run automatically every day at 02:00 UTC and send an email notification showing whether the run succeeded or failed.
  
- **Data Quality:** The company wants to make sure that the daily data is valid before processing it. The platform should check for missing columns, null values, duplicate records, invalid data types, and incorrect dates and times. Each daily file should meet a minimum quality score of 80%. If the quality is below 80%, the batch should be rejected and moved to quarantine. Also, if any order detail refers to an OrderID that does not exist in the daily orders or historical data, the entire batch should be rejected and moved to quarantine.
- **Analytics and Reporting:** The company wants daily_sales, product_performance, and customer_summary aggregation tables. Moreover, they want 2 Power BI reports. One report is to help company owners and sales managers understand sales performance and profitability, and to identify areas of improvement or decline, and the other one is to monitor new sales transactions and track sales activity in near real-time.
- **Data Access and Security:** Regional managers should only be able to see sales data for their assigned regions in Power BI. For example, a manager responsible for a region should see only that region's sales data, while a manager from another region should see only their own region's data. Access should be controlled based on the user's account. They should also be restricted from accessing sensitive customer information, such as age, city, and gender. 
- **Real-Time Analytics:** The company wants to monitor new sales transactions without waiting for the daily batch. The report should show the number of orders, total revenue, and units sold. It should also display recent transactions with their products, quantities, and timestamps, along with a chart showing how revenue changes over time.
- **Monitoring and Reliability:** The company wants to make sure the daily data pipelines run successfully and that any issues can be detected quickly. If something goes wrong, the system should send an email notification and keep failed files in quarantine for investigation. The pipelines should also be able to run again without creating duplicate records.

## 4. Proposed Solution
To meet these requirements, I built a data platform using Microsoft Fabric. The solution supports an initial full load, daily incremental updates, and near real-time sales monitoring.
For batch processing, the data goes through Bronze, Silver, and Gold layers in the Lakehouse. A separate Fabric Warehouse stores the sales data in a star schema for historical reporting in Power BI.
For real-time monitoring, simulated POS transactions are sent through Eventstream to Eventhouse and displayed in a separate Power BI report.

![Retail Data Architecture](docs/diagrams/Retail%20Data%20Architecture.png)

### My Role

As a Data Engineer, my responsibility is to build a data platform using Microsoft Fabric. This includes creating pipelines to load historical and daily sales data, cleaning and transforming the data, and loading it into a data warehouse. Moreover, setting up data validation, building a real-time sales pipeline, and preparing the data for reporting in Power BI.

## 5. Architecture Overview

The platform has three main data processing flows: initial full load, daily incremental processing, and real-time streaming.


### Initial Full Load Architecture
The initial full load is a one-time manual process that loads all five source tables into the Lakehouse for the first time.

![Initial Full Load Architecture](docs/diagrams/Initial%20Full%20Load%20Architecture.png)


### Daily Incremental Load Architecture
The daily pipeline processes new and updated orders and order details from Azure Blob Storage. It first copies and validates the incoming files before updating the Bronze, Silver, and Gold tables. It also updates the Warehouse with new dates and sales records, or changes to existing sales. If validation fails, both files are moved to quarantine, and an email notification is sent.

![Incremental Load Architecture](docs/diagrams/Incremntal%20Load%20Architecture.png)

### Real-Time Streaming
The real-time streaming process uses a POS simulator to generate new sales transactions. These transactions are sent through Eventstream and stored in Eventhouse. A separate Power BI report uses DirectQuery to monitor new orders, revenue, and units sold in near real-time, without waiting for the daily batch process.


### Warehouse Star Schema
The Warehouse uses a star schema with one fact table, "fact_sales", and three dimension tables "dim_customer", "dim_product", and "dim_date". The fact table stores sales transactions, while the dimension tables provide information about customers, products, and dates.

![Warehouse Star Schema](docs/diagrams/Warehouse%20Star%20Schema.png)


## 6. Technology Stack

| Technology | Purpose |
|---|---|
| Microsoft Fabric | Data platform |
| Azure Blob Storage | Source data storage |
| Fabric Data Pipelines | Orchestration |
| PySpark | Data transformation |
| Delta Lake | Lakehouse tables |
| Fabric Warehouse | Dimensional data modeling |
| SQL | Data loading and transformations |
| Eventstream | Real-time ingestion |
| Eventhouse | Real-time analytics |
| Power BI | Semantic modeling and reporting |


## 7. Data Engineering Implementation

### 7.1 Bronze Layer
The Bronze layer is the first step in the Lakehouse. It stores the data from the original CSV files as Delta tables, keeping it close to its original form without cleaning or any changes.
During the initial full load, all five tables (customers, products, categories, orders, and order_details) are loaded into Bronze. After that, only orders and order_details are updated through the daily incremental pipeline.

### 7.2 Silver Layer

In the Silver layer, the data from the Bronze layer is cleaned and prepared for further use. This includes removing duplicate records, handling missing values, converting columns to the correct data types, and making sure the data is consistent.

During the initial full load, all five tables are processed and stored as Silver Delta tables. After that, the daily incremental process updates only orders and order_details, adding new records and updating existing ones using MERGE.

The cleaned data is then used to build the Gold aggregation tables and the Warehouse star schema.

### 7.3 Gold Layer
The Gold layer uses the cleaned data from Silver to create three aggregation tables: daily_sales, product_performance, and customer_summary.
These tables summarize the data to make it easier to analyze daily sales, product performance, and customer activity without going through individual transactions.
During the initial full load, all three tables are created using historical data. After that, the daily incremental process updates them with new and changed sales data.

### 7.4 Warehouse and Star Schema

The retail_Lakehouse  uses a star schema with one fact table (fact_sales) and three dimension tables (dim_customer, dim_product, and dim_date).

The fact_sales table stores sales information, such as quantity, revenue, and profit. The dimension tables provide details about customers, products, and dates. They are connected to the fact table using surrogate keys instead of the original IDs.

During the initial full load, all four tables are populated. After that, the daily incremental process updates only dim_date and fact_sales, while dim_customer and dim_product remain unchanged.

### 7.5 Full Load

The initial full load is done manually to load the historical data for the first time. All five source tables are loaded into Bronze, then cleaned and transformed into Silver.

After that, the Gold aggregation tables are created using the Silver data. The Warehouse is also loaded from Silver, starting with dim_customer and dim_product, followed by dim_date and fact_sales.

Once the full load is completed, the data is ready for historical reporting in Power BI. Future updates are handled by the daily incremental pipeline.


### 7.6 Daily Incremental Load

The daily incremental pipeline processes only orders and order_details. It copies the new CSV files from Azure Blob Storage to the retail_Lakhouse and checks their quality before processing.

If validation passes, the pipeline updates the Bronze, Silver, and Gold tables. It uses MERGE to add new records and update existing ones instead of reloading all the data.

The pipeline also updates the retail_warehouse by adding new dates to dim_date and inserting or updating sales records in fact_sales.

The process is designed to be idempotent, which means running the same batch again should not create duplicate records. If validation fails, both files are moved to quarantine, and an email notification is sent.

## 8. Data Quality and Error Handling
Before processing the daily files, the pipeline runs a validation notebook to check the data quality. It checks for missing columns, null values, duplicates, invalid data types, dates, times, and incorrect values.
Each file must have a quality score of at least 80% as a requirement from the business. The notebook also checks that every OrderID in order_details exists either in the daily orders file or in the historical Silver table. If an order reference is missing or there is a missing required column. Then, the entire batch is rejected, even if the quality score is above 80%.
If validation fails, both daily files are moved to quarantine, and an email notification is sent. The pipeline also sends email notifications when technical failures happen or when processing completes successfully.
Failed batches can be investigated, corrected at the source, and processed again. The pipeline supports rerunning the same batch without creating duplicate records.

## 9. Real-Time Analytics

For real-time analytics, I created a POS simulator that generates new sales transactions to simulate purchases happening in a retail business.
The transactions are sent to Microsoft Fabric Eventstream, which receives the data and sends it to Eventhouse for storage and analysis.
I use KQL to query the data, calculate sales metrics, and track recent transactions. A separate Power BI report connects to Eventhouse using DirectQuery to show revenue, orders, units sold, and sales activity over time. This allows the company to monitor new sales without waiting for the daily batch process.

## 10. POS Simulator
The real-time component includes a Python-based POS simulator that generates simulated sales transactions. These events are sent to Fabric Eventstream and then processed in Eventhouse for near real-time analytics.
The simulator provides a way to test the streaming pipeline without requiring a connection to a real point-of-sale system.
The Python source code is included in this repository so the simulation logic can be reviewed.

## 11. Semantic Model and Power BI
For historical reporting, I created a semantic model connected to the Fabric Warehouse using Direct Lake. The model includes relationships between fact_sales, dim_customer, dim_product, and dim_date.
I also created DAX measures to calculate total revenue, total profit, units sold, and profit margin. These measures are used in the Executive Overview report to analyze sales trends and compare product categories.
For real-time reporting, I used DirectQuery to connect Power BI to Eventhouse. The Real-Time Operations report shows new transactions, revenue, orders, and units sold in near real-time.
The project has two Power BI reports: one for analyzing historical sales performance and another for monitoring new sales activity.


## 12. Security and Governance

Access to the Fabric workspace and Warehouse is managed through permissions and database roles. The historical report is intended to restrict each Regional Manager to their assigned region.

Row-Level Security and Column-Level Security are applied at the Warehouse and Semantic Model levels to enforce regional boundaries and restrict access to sensitive customer details, such as Age, City, and Gender.

## 13. Monitoring and Performance

The daily pipeline runs automatically at 02:00 UTC. Pipeline runs can be monitored through the Microsoft Fabric Monitoring Hub, pipeline run history, and notebook outputs.

The pipeline sends email notifications when processing completes successfully, when data validation fails, or when a technical error occurs. The Copy Data activities also have three retry attempts with a 30-second interval to help handle temporary failures.

To improve efficiency, the daily process uses incremental updates instead of reloading all historical data. Delta MERGE operations insert new records and update existing ones, while idempotent processing helps prevent duplicates when a batch is rerun. The ProcessDate parameter also allows a specific date to be reprocessed when needed.

The pipeline's validation and failure-handling paths, email notifications, and rerun behavior have been tested. Formal performance benchmarks have not yet been completed, so execution-time and capacity targets have not been defined. As data volumes grow, pipeline duration, activity failures, and Fabric capacity usage should be monitored to identify performance issues.

## 14. Testing and Validation
**Valid data:** Confirmed that files passing validation can continue through the processing pipeline.

**Missing column:** Tested a file with the required OrderTime column removed. The validation notebook detected the missing column, rejected the batch and sent it to quarantine.

**Data quality failure:** Checked that invalid batches follow the failure path instead of continuing to the transformation steps.

**Email notifications:** Tested notifications for successful processing, validation failures, and technical failures.

**Idempotency:** Reran the same batch to check that incremental processing does not create duplicate records.


## 15. Data Source
The historical retail data used in this project comes from the [Retail Data dataset on Kaggle](https://www.kaggle.com/datasets/hyerdrac/retail-data).

