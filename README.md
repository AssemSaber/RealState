# **Project Overview**
   **This project demonstrates a modern data engineering architecture designed to process real estate market data using `a Hybrid Lambda-style approach (Real-time Streaming & Batch Pipelines)`. The system focuses on enriching raw data with Machine Learning to deliver high-priority insights to business stakeholders.**
   **Leveraging a `Lakehouse Architecture (Delta Lake)`, the system follows the `Bronze–Silver–Gold` pattern to ensure data reliability, ACID transactions, and schema enforcement**
   **The processed data is activated to:**
  -  **`Trigger Real-Time AI Alerts:`** 
     -   **`Urgent Support Path`** : If the AI identifies a **`"Complaint" or "Maintenance Issue"`** the record triggers an immediate high-priority alert to the **Urgent Support Team via the Telegram** Bot API.
     -   **`Sales Lead Path`** : If the AI identifies a **`"Question" or "Pricing Inquiry"`**, the data is enriched with property details and routed to the **Sales Team Telegram** Bot API.
 -   **`Drive Business Intelligence:`** Providing historical market analytics through **`Power BI dashboards.`**
  -  **`Conversational AI Discovery`** Powering an Intelligent **`Chatbot Interface (GPT-style) `** that allows users to interact with the real estate market through natural dialogue.
----
## **The Business Problem**
 Real estate platforms handle massive volumes of **unstructured and structured data**, often leading to three critical failures:
  -  **Response Latency**: Urgent customer grievances are lost in batch processing cycles, damaging brand reputation.
  -  **Missed Revenue**: High-intent sales inquiries are treated as static comments rather than live leads, leading to lost conversion opportunities.
  -  **Search Friction & Information Overload (The Chatbot Problem):** Traditional real estate platforms rely on `Static Filtering` (Price, SQM, Location).
-------
## **Architecture Overview**
![System Architecture photo](images/architecture_realstate.gif)
-------
# Table of Contents

### Core Architecture
- [Project Overview](#project-overview)
- [The Business Problem](#the-business-problem)
- [Architecture Diagram](#architecture-overview)

### Data Engineering
- [Real-Time Interaction Stream](#a-real-time-interaction-stream-unstructured)
- [Batch Ingestion & Medallion Layers](#b-batch-ingestion-structured)
- [Engineering Challenges & Solutions](#challenges-overcome-engineering)
- [Spark Performance Optimization](#challenges-in-spark)
- [Successful Full Pipeline Execution](#successful-full-pipeline-execution)

### Delivery & Insights
- [Power BI Dashboards](#power-bi-dashboards)
- [AI Chatbot Integration](#chatbot-interface)
- [Video Demo](#video-demo)
- [Full Tech Stack](#tech-stack)


## **A. Real-Time Interaction Stream (Unstructured)**
#### This layer serves as the "Brain" of the real-time pipeline, utilizing Apache Spark Structured Streaming and Spark MLlib to perform high-speed inference and multi-dimensional data enrichment.
  - **Source**: User-generated Comments & Complaints
  - **Mechanism**: Ingested via Azure Cosmos DB **Change Feed** (CDC).
  - **Engineering Detail**: This stream captures high-velocity, unstructured Arabic text. It is the primary trigger for the Real-Time AI Routing logic, allowing the system to react to customer
  - **Real-Time AI Inference (NLP)**:The system applies a specialized **Arabic-BERT model** to every incoming event to extract intent and sentiment, where Automatically categorizing messages as **`"Urgent Complaint,"`** or **`"Sales Lead."`**
  - **Data Enrichment**: A raw stream is often **missing the context needed for a team** to take action, so this layer performs a Streaming-to-Static Join to "hydrate" the event with critical metadata: `Phone client, City, District, price, area, and "comment or complaint"`
  - **Sending in near-realtime to Telegram alerts**
## Sales Team
![System Architecture photo](images/sales.jpeg)
## Urgent Team
![System Architecture photo](images/urgent.jpeg)
----
## **B. Batch Ingestion (Structured)**
- [Data Dictionary:](https://www.kaggle.com/datasets/mohdph/saudi-arabia-real-estate-dataset)
- First of all, we started with `Exploratory Data Analysis (EDA)`
- **Multi-Stage Ingestion** `(Bronze Layer)`
  - **Source:** External REST API
  - **Mechanism:** Scheduled Batch Ingestion via Azure Data Factory (ADF)
  - **Engineering Detail:** Data is ingested into ADLS Gen2 in its raw format
- **Data Transformation** `(Silver Layer)`
    - **Data Deduplication**
    - **Schema Enforcement:** using Delta Lake to enforce a consistent schema and prevent data corruption caused by unexpected API schema changes.
    - **Standardization & Normalization:** Converting abbreviated values and inconsistent labels into standardized and meaningful names.
    - **Missing Value Handling:** `Numerical fields` are filled using the `median` within each logical group, while `categorical` fields are imputed using the most frequent value `mode` within the same group. 
- **Business-Ready Analytics** `(Gold Layer)`
    - **Star Schema Modeling:** Data is organized into optimized Fact and Dimension tables.
    - **Historical Data for Users:** slowly changing dimension (SCD2)
    - ![System Architecture photo](images/DWH_RealState.png)
---

### We use Table Format: 
  - **Using Delta Lake to support ACID transactions, data auditing, schema evolution, schema enforcement, and table versioning.**
    
<a name="challenges-overcome-engineering"></a>
## ${\textsf{\color{blue} Challenges Overcome Engineering}}$

**${\textsf{\color{red}Challenge}}$:** Full Load Across All Layers

**${\textsf{\color{green}Solution}}$:** the pipeline utilizes `Delta Change Data Feed (CDF).`
  - **Efficiency:** By enabling delta.enableChangeDataFeed = true, we only process the changes (inserts/updates/deletes) between versions.
  - **Cost Optimization:** This reduces I/O overhead and `no need full MERGE operations` because we simply start with condition about version if ( last_version<current_version )
  - **Idempotency:** Ensures that only failed tasks are retried during pipeline failures without reprocessing successfully completed steps.

**${\textsf{\color{red}Challenge}}$:** `hardcoded` to manage all metadata for all tables that apply incremental loading using versions of CDF

**${\textsf{\color{green}Solution}}$:** `Custom Package` Architecture (Abstraction Layer) To ensure `reusability and maintainability`

**${\textsf{\color{red}Challenge}}$:** `High coupling` between transformation logic within each layer.

**${\textsf{\color{green}Solution}}$:** Introducing `micro-layers` inside each layer to isolate responsibilities, improve modularity, and simplify maintenance.

**${\textsf{\color{red}Challenge}}$:**  Spark usually reads all data in the Bronze layer

**${\textsf{\color{green}Solution}}$:** Configure Azure Data Factory to write the data `partitioned by` `year`, `month`, and `day`, allowing Spark to prune unnecessary partitions during reads

<a name="challenges-in-spark"></a>
### **${\textsf{\color{red}Challenges in Spark}}$:**
  - The transformation took  **${\textsf{\color{red}25 minute}}$** and with the last transformation occured **${\textsf{\color{red}out of memory}}$**

    **${\textsf{\color{red}Challenge}}$:**
    - we avoided the `SortMergeJoin` by using a `BroadcastHashJoin`
        - ![System Architecture photo](images/merge.png)
        
    **${\textsf{\color{green}Solution}}$:**
      - ![System Architecture photo](images/broadcast.png)

    **${\textsf{\color{red}Challenge}}$:**
    -  Excessive `repartitioning` occurred across the cluster and within `partitions` because it was called inside the function. The intention was to sort the data at the partition level to make the `groupBy operation easier and more efficient`
    
    - ![System Architecture photo](images/repartition.png)
      
    **${\textsf{\color{green}Solution}}$:**
       - The best practice was to perform a `single repartition` in the main code, then `cache the DataFrame`. Also, there was `no need to sort` the data before aggregation because Spark used a `HashAggregate`
      - ![System Architecture photo](images/cache.png)
        
    **Finally, we got**
    - ![System Architecture photo](images/spark_optimization_2.jpeg)
----

### Successful Full Pipeline Execution 

   - ![System Architecture photo](images/successul_run.jpeg)
    
   - ![System Architecture photo](images/DBX_flow.jpeg)

----
## Data Consumption
   - ### Power BI dashboards
      - ![System Architecture photo](images/screen_1.png)
      - ![System Architecture photo](images/screen_2.png)
      - ![System Architecture photo](images/screen_3.png)
      - ![System Architecture photo](images/screen_4.png)
   - ### Chatbot  
      Features
        - Multi-language support (Arabic and English)
        - Conversational preference gathering (Buy vs Rent, City, Property Type, Budget)
        - Real-time property search using CSV data
        - Property detail retrieval and filter refinement suggestions
        - Integration with Azure OpenAI and LangGraph

----
## Video Demo 
- [**Watch the Demo!**](https://drive.google.com/file/d/15V5Mfz55CDbDGTdL61Z16O9k_ujh9-dS/view?usp=sharing)
  
----
### Tech Stack

**Data Orchestration & Ingestion**
  - **Azure Data Factory (ADF):** Orchestrates complex ETL workflows, managing batch ingestion from REST APIs.
  - **Azure Cosmos DB (Change Feed):** Acts as the low-latency entry point for streaming events, capturing live comments and complaints.
    
**Distributed Processing & AI**
  - **Azure Databricks (PySpark):** The core engine for distributed data transformation and Medallion layer management.
  - **Spark MLlib:** Powers the real-time inference engine using BERT-based models to classify sentiment and intent from incoming text.
    
**Storage & Unified Data Layer**
  - **Azure Data Lake Storage (ADLS Gen2):** Scalable, hierarchical storage for raw and processed assets.
  - **Delta Lake:** Provides ACID transactions, schema enforcement, and Time Travel capabilities across the Bronze, Silver, and Gold layers.

**Security & Governance**
  - **Azure Key Vault:** Centralized management of service principals, connection strings, and API secrets.
    
**Analytics & Delivery**
  - **Power BI:** Dimensional modeling (Star Schema) for executive-level market reporting.
  - **Telegram Bot Integration:** Event-driven alerting system that routes urgent maintenance tickets and sales leads instantly.

