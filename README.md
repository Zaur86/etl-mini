# etl-mini

## Overview

`etl-mini` is a modular ETL framework built in Python, designed to support robust and customizable data pipelines. It includes a flexible architecture for extracting data from various sources (APIs, S3, Elasticsearch), processing and transforming it, and loading it into a PostgreSQL-based DWH. The project supports standard and custom pipelines, organized for rapid prototyping and production-ready deployments.

## Project Structure

- **main.py**  
  Entry point for running pipelines and initializing the application.

- **app/**  
  Core settings, error handling modules, utility functions, and custom warning definitions.

- **config/**  
  Configuration files including API templates and schema definitions.

- **models/**  
  Definitions for DWH tables, query templates, field mappings, and helper functions.

- **services/**  
  Contains implementations of pipeline stages and data source integrations:
  - **sources/**: Services for fetching data from APIs, S3, or Elasticsearch.
  - **pipelines/**: Modular pipeline definitions organized by purpose (`external_source_to_raw`, `external_raw_to_dwh`, `internal_raw_to_dwh`).
  - **transformers/**: Data transformation utilities (e.g., enrichment, format conversion).

- **scripts/**  
  Script directory for ad-hoc and scheduled runs of specific pipeline components.

- **smoke_tests/**  
  Unit tests and config templates for validating pipeline modules and data source integrations.

- **routers/**  
  Placeholder for future FastAPI-based routing (currently empty).

- **logs/**, **tmp/**  
  Directories for storing logs, temporary exports, and development artifacts.

- **requirements.txt**  
  Python dependency list for setting up the project environment.


 ## Architecture Overview

The project is structured around a layered and extensible ETL architecture, with clear separation between abstract base classes (interfaces) and their concrete implementations. This enables flexibility and reuse across different data sources and pipeline types.

### Core Concepts

#### 1. **Base Services (`services/sources/base/`)**

These define abstract classes for interacting with various data layers. Each base class is designed to be extended by a concrete implementation.

- `external_source_service.py`:  
  Abstract interface for pulling data from external APIs or systems.

- `external_raw_storage_service.py`:  
  Defines the interface for raw data storage before transformation (e.g., S3).

- `internal_raw_storage_service.py`:  
  Abstract class for reading from internally collected raw data sources (e.g., Elasticsearch).

- `dwh_service.py`:  
  Interface for pushing data into the final DWH (e.g., PostgreSQL).

#### 2. **Service Implementations (`services/sources/implementations/`)**

These are concrete classes that implement the logic defined in the base services:

- `external_source/simple_api_service.py`:  
  Pulls data from JSON-based HTTP APIs.

- `external_raw_storage/s3_service.py`:  
  Saves and retrieves raw data files from AWS S3.

- `internal_raw_storage/elasticsearch_service.py`:  
  Reads raw event data from an Elasticsearch cluster.

- `dwh/postgresql_service.py`:  
  Loads processed data into a PostgreSQL data warehouse.

#### 3. **Pipeline Abstractions (`services/pipelines/`)**

Each pipeline type has a structured directory with:
- `standard_pipeline.py`: The default logic for handling extraction, transformation, and loading.
- `custom_pipelines/`: Directory for user-defined logic that overrides or extends standard behavior.
- `runs/`: Folder structure for organizing execution runs and artifacts (e.g., temp files, configs).

Pipeline types include:
- `external_source_to_raw`:  
  Extract data from external sources and save it to raw storage (e.g., API → S3).

- `external_raw_to_dwh`:  
  Load data from raw storage into the data warehouse (e.g., S3 → PostgreSQL).

- `internal_raw_to_dwh`:  
  Process internally collected data and load into the data warehouse (e.g., Elasticsearch → PostgreSQL).

#### 4. **Transformers (`services/transformers/`)**

- `base_transformer.py`:  
  Abstract base class for all data transformers.

- `pandas_select_and_enrich.py`:  
  Enriches and formats data using Pandas.

- `tsv_converter.py`:  
  Converts structured data to TSV format if needed before loading.

### Execution Flow Example

For a standard pipeline such as `internal_raw_to_dwh`:
1. `internal_raw_storage_service` fetches data from Elasticsearch.
2. Data is processed using `pandas_select_and_enrich`.
3. `postgresql_service` loads the transformed data into the PostgreSQL DWH.

Custom pipelines can override or supplement this behavior by placing custom logic inside the corresponding `custom_pipelines/` subdirectory.
