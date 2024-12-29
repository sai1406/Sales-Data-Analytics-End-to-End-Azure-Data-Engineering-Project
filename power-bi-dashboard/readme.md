Sales Data Dashboards

Overview

This repository contains Power BI dashboards designed for comprehensive analysis of sales data. The dashboards are connected directly to Databricks Gold Tables in the Databricks Catalog, leveraging clean and enriched data for real-time insights.

Dashboards

The following dashboards are included in the SalesDataDashboard.pbix file:

1. Customer Count for Each Product

Purpose: Displays the number of unique customers for each product.
<img width="596" alt="image" src="https://github.com/user-attachments/assets/d172a5d3-2e6b-405d-a470-7d1bd58dcf49" />


2. Top Sold Products

Purpose: Highlights the products with the highest sales volume.
<img width="604" alt="image" src="https://github.com/user-attachments/assets/a7ba68b4-4002-48a1-82ba-746598503e4d" />



3. Customer Lifetime Value

Purpose: Calculates the lifetime value (LTV) of customers based on historical sales data.
<img width="606" alt="image" src="https://github.com/user-attachments/assets/3e7c6f82-7be1-4b69-bd3b-ca0da6005a66" />



4. Yearly Revenue by Products

Purpose: Summarizes annual revenue contributions for each product.
<img width="599" alt="image" src="https://github.com/user-attachments/assets/1076d734-c76e-40ad-b02f-ec2ceae87f7f" />



Data Source

The dashboards query data directly from the Databricks Gold Tables in the Databricks Catalog. The Gold Tables contain fully cleaned and transformed data prepared using Delta Lake concepts.
