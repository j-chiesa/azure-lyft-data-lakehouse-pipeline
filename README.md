# Lyft Data Lakehouse Pipeline
This project entails building a robust data pipeline for processing hypothetical Lyft trip data. It retrieves a `.parquet` file containing trip data from the previous two months from the [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) web page and loads it into Azure Data Lake Storage Gen2. The data is then processed using a medallion architecture within Azure Databricks (Spark), creating a Delta Lake for each layer (Bronze, Silver, and Gold). This structured data is subsequently leveraged to establish a Delta Lakehouse in a serverless SQL pool within Azure Synapse Analytics. Finally, Power BI connects to the Gold layer in Synapse, allowing for insightful visualizations that facilitate data-driven decision-making.

## 📑 Table of Contents
1. [Architecture](#architecture)
2. [Components](#components)
3. [Data Model](#data-model)
4. [Processing Pipeline](#processing-pipeline)
5. [Implementation](#implementation)
   - [Azure Data Lake Storage Gen2](#azure-data-lake-storage-gen2-configuration)
   - [Azure Data Factory](#azure-data-factory-configuration)
   - [Azure Databricks](#azure-databricks-configuration)
   - [Azure Synapse Analytics](#azure-synapse-analytics-configuration)
   - [Power BI](#power-bi-configuration)
7. [Visualization](#visualization)
8. [Notes and Credits](#notes-and-credits)

### Architecture 
This solution is based on the medallion architecture (Bronze, Silver, and Gold layers), optimized for cloud storage and processing using Azure. Databricks is utilized for processing the data through these layers, leveraging Delta Lake to enable efficient data management and ensure data integrity. Below is an outline of the main components:
![Architecture](./assets/architecture-diagram.png)

### Components
- **Orchestatrion:** Azure Data Factory is used to extract data from the website and load it into Azure Data Lake Storage Gen2. It sets a variable that allows for the dynamic retrieval of the file containing data from two months prior. Additionally, Data Factory orchestrates the execution of Azure Databricks notebooks, ensuring smooth processing of the data pipeline.
- **Storage:** The data is initially stored in Azure Data Lake Storage Gen2, which is then converted into a Delta Lake in the Bronze layer. Each layer (Bronze, Silver, and Gold) has its own partitioning structure by year and month, enabling efficient data management and retrieval.
- **Transformation**:

### Data Model

### Processing Pipeline

### Implementation

### Visualization

### Notes and Credits
