# Spark-Scala-Stocks

### A back-end demo project that:

- **Accesses Stock Price Data**:  Uses Yahoo Finance’s API to fetch historical stock price data, including prices, volumes, and other market information.

- **Loads Data into Spark**: Imports years' worth of stock price data into Apache Spark’s DataFrame API for efficient processing.

- **Computer Technical and Quantitative Analysis**
    - Currently:
        - **Daily Moving Average**

    - More to Come:
        - Exponential Moving Average
        - MACD and Grafana ???

### Detailed Breakdown:

- Accessing Stock Price Data:
Use Yahoo Finance API to pull stock data such as opening, closing, high, and low prices, and volumes.
        [Example endpoint](https://query1.finance.yahoo.com/v7/finance/download/SPY?period1=1709272800&period2=1717217999&interval=1d&events=history).

- Loading Data into Spark:
        Convert the fetched data into a format that Spark can process.  Currently, the data is in-memory CSV format using:
        `spark.createDataFrame(spark.sparkContext.parallelize(rowData), schema)`
