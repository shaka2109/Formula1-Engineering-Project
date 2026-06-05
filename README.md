## Overview

This project implements an end-to-end Data Engineering solution for Formula 1 race data using Azure Databricks, Azure Data Lake Storage (ADLS), Delta Lake, Unity Catalog, and Databricks Asset Bundles (DABs). The solution processes Formula 1 datasets using an incremental batch ingestion strategy, ensuring that only newly arrived data is processed while previously loaded batches remain untouched. The architecture follows the Medallion Design Pattern (Bronze, Silver, Gold) and includes an orchestration framework responsible for identifying, tracking, and processing new monthly batches automatically.

### Highlights

- Incremental processing framework
- Medallion Architecture
- Azure Data Lake Storage Gen2
- Unity Catalog & External Locations
- Mixed-format ingestion (CSV, JSON, Multiline JSON)
- Nested folder processing
- Automated batch orchestration
- Databricks Workflows
- Databricks Asset Bundles (DABs)
- GitHub Actions CI/CD

## Business Scenario

Formula 1 race data is delivered monthly as folders containing multiple datasets.

### Data Sources

Datasets are delivered in multiple formats and structures, simulating real-world ingestion scenarios.

Supported formats include:

- CSV files
- JSON files
- Multiline JSON files
- Nested folder structures

Examples:
```text
      2025-01/
      ├── circuits.csv
      ├── constructors.json
      ├── drivers_multiline.json
      ├── results/
      │   ├── part-0001.json
      │   ├── part-0002.json
      │   └── ...
```

The ingestion framework dynamically handles each file format and applies the appropriate parsing strategy before loading the data into the Bronze layer. As new monthly folders arrive, the system must:

- Detect new batches.
- Avoid reprocessing historical data.
- Process only newly arrived data.
- Maintain historical records.
- Update analytical datasets automatically.

## Technologies Used

- Azure Databricks
- PySpark
- Delta Lake
- Unity Catalog
- Azure Data Lake Storage Gen2 (ADLS)
- Databricks Workflows
- Databricks Asset Bundles (DABs)
- GitHub Actions
- Git
- SQL

## Medallion Architecture

### Bronze Layer

The Bronze layer ingests raw Formula 1 datasets directly from ADLS.

Features:

- CSV ingestion
- JSON ingestion
- Multiline JSON ingestion
- Recursive folder reading
- Incremental batch processing
-	Apply correct schema
-	Add audit columns
-	Store data in delta format
-	Starting with full load, but extending with incremental load

### Silver Layer

The Silver layer standardizes and cleanses the data.

Features:

-	Clean and standardize data
-	Apply consistent name and reshape structure
-	Remove unnecessary columns and handle basic data quality
-	Preserve business keys across layers
-	Prepare datasets for gold-layer analytics.

### Gold Layer

The Gold layer provides analytical datasets optimized for reporting and analytics.

Features:

-	Driver standing for each season
-	Constructor standing for each season
-	Analise dominant drivers and teams
-	Support recent and historical analysis
-	Optimised for reporting and analytical queries

## Incremental Batch Processing

The project implements a custom batch-control framework.

Each monthly folder after 2025-1 represents a unique batch since 2025-01 keeps historical data as well:
```text
    2025-01
    2025-02
    2025-03
    .
    .
    .
```

The orchestration layer compares:

- Available batches in ADLS
- Previously processed batches

Only unprocessed batches are selected for execution.

### Batch Identification Logic

```text
           ADLS Folders
                │
                ▼
        Identify Next Batch
                │
                ▼
          Has New Batch?
             /      \
           Yes       No
            │        │
            ▼        ▼
         Run ETL   Stop
```

## Orchestration Framework

The orchestration workflow consists of four notebooks:

### 1. Identify_Next_Batch

- Reads available folders from ADLS.
- Identifies unprocessed batches.
- Returns the next available batch.

### 2. Create_New_Batch

- Registers the execution.
- Creates batch metadata.
- Initializes processing status.

### 3. Complete_Batch

- Marks the batch as successfully processed.
- Updates execution metadata.

### 4. Control_Batch

- Maintains processing history.
- Prevents duplicate executions.

## Databricks Jobs

The solution deploys two Databricks Jobs using Databricks Asset Bundles (DABs).

### Formula1 Full Pipeline
```python
        Bronze
          ↓
        Silver
          ↓
        Gold
```

Responsible for processing and transforming Formula 1 datasets through the Medallion Architecture.

### Formula1 Pipeline Orchestration
```python
    Identify Batch
          ↓
     Create Batch
          ↓
 Execute Full Pipeline
          ↓
    Complete Batch
          ↓
    Control Batch
```

Responsible for managing incremental execution and batch lifecycle control.

## CI/CD Implementation

Continuous Integration and Continuous Deployment (CI/CD) were implemented using GitHub Actions and Databricks Asset Bundles.

Deployment Strategy:

- Push to develop → Deploy DEV environment
- Pull Request merged into main → Deploy PROD environment

Automated validations:

- Bundle validation
- Resource deployment
- Job updates
- Workflow updates

## Repository Structure

```text
      Formula1-Incremental-Engineering-Project/
      │
      ├── databricks.yml
      │
      ├── notebooks/
      │   ├── Bronze/
      │   ├── Silver/
      │   ├── Gold/
      │   └── Orchestration/
      │
      ├── resources/
      │   ├── formula1_full_pipeline.yml
      │   └── formula1_pipeline_orchestration.yml
      │
      └── .github/
          └── workflows/
              └── databricks-cicd.yml
```

## Key Features

- Incremental batch ingestion
- Medallion Architecture
- Delta Lake processing
- Azure Data Lake integration
- Unity Catalog governance
- External Locations
- Workflow orchestration
- Databricks Asset Bundles
- CI/CD automation
- Multi-environment deployment

## Results

The solution successfully demonstrates:

- Incremental data processing.
- Automated batch lifecycle management.
- End-to-end orchestration.
- Production-style deployment using Databricks Asset Bundles.
- CI/CD automation with GitHub Actions.
- Scalable Data Engineering architecture using Azure Databricks.



