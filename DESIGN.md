STATEMENT OF PURPOSE

- All Python code should relate automation and Apache Airflow.
  - Initialization: run __jars__ related to bootstrapping.
  - Data activities: run CRUD __jars__
- All Scala code should relate to data related activities to leverage Apache Spark.
  - Data validation
    - Timestamps are in correct chronological order, typically past -> future 
    - Timestamps placement occurs are when needed.  No timestamps mark future events where only present or past is acceptable
  - Data quality
  - Data completeness