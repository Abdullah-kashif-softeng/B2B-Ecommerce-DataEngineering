B2B Data Platform: Fresh Requirements Document

Project Vision

Build a comprehensive ETL/ELT data pipeline for a B2B e-commerce platform on Databricks. The platform must:

Ingest data from 3 diverse sources (PostgreSQL, web logs, marketing spreadsheets)
Transform & stage raw data in Databricks (Delta Lake, Unity Catalog) with CDC tracking
Build a warehouse with a star schema optimized for analytics
Surface insights through 10-15 KPI-driven business reports
Automate & orchestrate the entire pipeline with monitoring and notifications

Data Sources (Input Layer)

Source 1: PostgreSQL: Operational B2B Database (read via Lakehouse Federation / JDBC)

Companies (suppliers & buyers with locations, CUIT tax IDs, ratings)
Orders & order items (products, quantities, prices, status)
Products (SKU, name, category)
Suppliers & supplier-product mappings (with pricing)
End customers (demographics, company affiliation)
Change tracking via updated_at timestamps (for CDC)

Source 2: Web Server Logs (Apache Combined Format / Unity Catalog Volumes, ingested with Auto Loader)

HTTP request logs (method, path, status, bytes sent)
Client IP addresses (for geolocation)
User-Agent strings (for device classification)
Authentication info (auth_user, session info)
Timestamps (request time)
Referer & path (for traffic analysis)

Source 3: Marketing Leads (Excel Spreadsheets / Unity Catalog Volumes, ingested with Auto Loader)

Lead ID, source, campaign name, UTM attributes
Geographic info (country, city)
Company profile (size, industry)
Lead quality metrics (lead score, estimated order volume)
Lead status (funnel stage)

Success Metrics: Required KPIs (10–15)

The solution should answer these business questions via Databricks SQL views:

Sales & Revenue (4 KPIs)

Monthly Revenue Trend: Gross and net revenue (excluding cancelled orders) by month with MoM growth %
Revenue by Company: Top buyer companies by revenue; identify top 20% of customers (Pareto analysis)
Revenue by Product/Category: Product performance; identify slow-movers and bestsellers
Gross Margin Analysis: Margin % per product/supplier; compare catalog price vs actual selling price

Customer Intelligence (3 KPIs)

Customer Lifetime Value (CLV): Total spend per buyer company; segment into cohorts
Customer Activity: Orders per customer, average order value (AOV), repeat purchase rate
Geographic Sales: Revenue and order count by country/city; identify growth regions

Web & Traffic Analytics (3 KPIs)

Traffic by Device Type: Desktop/Mobile/Tablet breakdown; device-specific conversion metrics
Geographic Web Traffic: Web sessions by country/IP geolocation; identify top traffic sources
Web Activity Quality: Error rates (4xx/5xx) by endpoint; identify problematic pages

Lead Quality & Sales Funnel (2 KPIs)

Lead Conversion Funnel: Lead source → orders; conversion rates by campaign/channel
Lead Quality vs Order Value: Correlation between lead score and actual order value

Supplier Performance (2 KPIs)

Supplier Revenue: Orders and revenue by supplier; supplier concentration (% from top N)
Supplier-Product Performance: Which supplier-product combinations perform best

Data Quality & Pipeline Health (1 KPI)

Pipeline Execution Metrics: Load times, error rates, data freshness; CDC watermark lag (from the logging Delta tables and Databricks Jobs run history / system tables)

Data Generation Requirements

The solution must generate realistic synthetic sample data including:

2 years of historical data (backfill)
Continuous daily ingestion (new orders, web logs, leads)
Seasonal patterns (Q4 peak, summer dip)
Weekly/daily patterns (weekday orders, morning traffic spikes)
Geographic distribution (concentrated in HQ locations)
Order cancellations (5-10% realistic rate)
Bot traffic (Googlebot, curl clients in web logs)
Multiple device types and browsers in user-agent strings
Deterministic seeding (repeatable reference data)

Deliverables

PostgreSQL DDL: Schema for B2B operational database
Databricks Staging DDL: Raw Delta tables (Unity Catalog) + logging
Databricks Warehouse DDL: Star schema (Delta tables) with dimensions, facts, references
Data Generation Scripts: Python generators (Databricks notebooks) for PostgreSQL, web logs, leads
10–15 KPI SQL Views: Actionable business reports (Databricks SQL)
ETL Notebooks / Python Tasks: Ingestion (Auto Loader / JDBC), staging loads, warehouse transforms, IP geolocation, notifications
Databricks Workflows (Jobs) YAML: Orchestration logic with error handling, retries, and failure notifications (defined as a Databricks Asset Bundle)
Documentation: README, deployment guide, query examples

Constraints & Considerations

Idempotent design: All warehouse loads use MERGE (Delta Lake MERGE INTO, safe to retry without duplicates)
CDC efficiency: Only extract changed rows from PostgreSQL using watermarks
Partition strategy: fact_* Delta tables partitioned by date for cost-efficient queries
Clustering: Dimensions clustered by common join keys (Delta Liquid Clustering / CLUSTER BY)
SCD Type 2: dim_company tracks version history for accurate historical attribution
No automated tests (pytest): Validate manually via end-to-end runs
No CI/CD: Deploy notebooks and jobs manually or via the Databricks CLI

Summary

This requirements document defines a production-grade B2B data analytics platform combining operational data (PostgreSQL), behavioral data (web logs), and marketing data (leads) into a unified analytical warehouse on Databricks serving 15 critical business KPIs across revenue, customer, traffic, lead, supplier, and pipeline health domains.