from google.cloud import bigquery
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from utils.config import CONFIG, CREDENTIALS_PATH

@task(name="create_bigquery_client", retries=0)
def create_client():
    client = bigquery.Client.from_service_account_json(
        str(CREDENTIALS_PATH),
        project=CONFIG['project-name']
    )
    print(f"✓ BigQuery client created for project {CONFIG['project-name']}")
    return client

@task(name="setup_bigquery_environment", cache_policy=NO_CACHE)
def setup_environment(client: bigquery.Client):
    dataset_id = f"{CONFIG['project-name']}.{CONFIG['dataset-name']}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)
    print(f"✓ Dataset {CONFIG['dataset-name']} verified/created")

@task(name="register_external_tables", retries=2, cache_policy=NO_CACHE)
def register_external_tables(client: bigquery.Client):
    print("Registering external tables with Hive partitioning...")
    dataset_id = CONFIG['dataset-name']
    bucket = CONFIG['bucket-name']
    
    fundamentals_schema = [
        bigquery.SchemaField("Ticker", "STRING"),
        bigquery.SchemaField("Company_Name", "STRING"),
        bigquery.SchemaField("Revenue", "FLOAT"),
        bigquery.SchemaField("Net_Income", "FLOAT"),
        bigquery.SchemaField("Pretax_Income_Loss_Adj", "FLOAT"),
        bigquery.SchemaField("Profit_Margin", "FLOAT"),
        bigquery.SchemaField("ROE", "FLOAT"),
        bigquery.SchemaField("ROA", "FLOAT"),
        bigquery.SchemaField("Debt_to_Equity", "FLOAT"),
        bigquery.SchemaField("Current_Ratio", "FLOAT")
    ]

    prices_schema = [
        bigquery.SchemaField("Ticker", "STRING"),
        bigquery.SchemaField("Date", "STRING"), 
        bigquery.SchemaField("Open", "FLOAT"),
        bigquery.SchemaField("High", "FLOAT"),
        bigquery.SchemaField("Low", "FLOAT"),
        bigquery.SchemaField("Close", "FLOAT"),
        bigquery.SchemaField("Adj_Close", "FLOAT"),
        bigquery.SchemaField("Volume", "INTEGER"),
        bigquery.SchemaField("Volume_Millions", "FLOAT"),
        bigquery.SchemaField("Daily_Return", "FLOAT")
    ]

    table_configs = {
        "stock_fundamentals_external": {
            "uri": f"gs://{bucket}/transformed/fundamentals/*",
            "partition_prefix": f"gs://{bucket}/transformed/fundamentals",
            "schema": fundamentals_schema
        },
        "stock_prices_external": {
            "uri": f"gs://{bucket}/transformed/prices/*",
            "partition_prefix": f"gs://{bucket}/transformed/prices",
            "schema": prices_schema
        }
    }

    for table_name, paths in table_configs.items():
        table_id = f"{CONFIG['project-name']}.{dataset_id}.{table_name}"
        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [paths["uri"]]
        
        if paths["schema"]:
            external_config.schema = paths["schema"]
            external_config.autodetect = False
        else:
            external_config.autodetect = True
        
        external_config.parquet_options.enable_list_inference = True
        hive_options = bigquery.HivePartitioningOptions()
        hive_options.mode = "STRINGS"
        hive_options.source_uri_prefix = paths["partition_prefix"]
        external_config.hive_partitioning = hive_options

        table = bigquery.Table(table_id)
        table.external_data_configuration = external_config
        client.delete_table(table_id, not_found_ok=True)
        client.create_table(table)
        print(f"✓ Registered external table: {table_name}")

@task(name="create_materialized_fundamentals_table", retries=2, cache_policy=NO_CACHE)
def create_fundamentals_table(client: bigquery.Client):
    print("Creating materialized fundamentals table...")
    dataset_id = CONFIG['dataset-name']
    table_id = f"{CONFIG['project-name']}.{dataset_id}.stock_fundamentals"
    
    query = f"""
    CREATE OR REPLACE TABLE `{table_id}` AS
    SELECT 
        Ticker, Company_Name, Revenue, Net_Income,
        Pretax_Income_Loss_Adj, Profit_Margin, ROE, ROA,
        Debt_to_Equity, Current_Ratio,
        CAST(Year AS INT64) as Year
    FROM `{CONFIG['project-name']}.{dataset_id}.stock_fundamentals_external`
    WHERE Revenue IS NOT NULL
    """
    query_job = client.query(query)
    query_job.result()
    table = client.get_table(table_id)
    print(f"✓ Created table {table_id}")
    print(f"  Rows: {table.num_rows:,}")
    return table_id

@task(name="create_materialized_prices_table", retries=2, cache_policy=NO_CACHE)
def create_prices_table(client: bigquery.Client):
    print("Creating materialized prices table...")
    dataset_id = CONFIG['dataset-name']
    table_id = f"{CONFIG['project-name']}.{dataset_id}.stock_prices"
    
    query = f"""
    CREATE OR REPLACE TABLE `{table_id}` AS
    SELECT 
        Ticker,
        CAST(Date AS DATE) as Date,
        Open, High, Low, Close, Adj_Close, Volume, Volume_Millions, Daily_Return,
        CAST(Year AS INT64) as Year,
        CAST(Month AS INT64) as Month
    FROM `{CONFIG['project-name']}.{dataset_id}.stock_prices_external`
    WHERE Close > 0
    """
    query_job = client.query(query)
    query_job.result()
    table = client.get_table(table_id)
    print(f"✓ Created table {table_id}")
    print(f"  Rows: {table.num_rows:,}")
    return table_id

@task(name="create_aggregated_views", retries=1, cache_policy=NO_CACHE)
def create_aggregated_views(client: bigquery.Client):
    print("Creating aggregated analysis views...")
    dataset_id = CONFIG['dataset-name']
    view_queries = {
        "annual_company_metrics": f"""
        CREATE OR REPLACE VIEW `{CONFIG['project-name']}.{dataset_id}.annual_company_metrics` AS
        SELECT 
            Ticker, Company_Name, Year,
            AVG(Revenue) as Avg_Revenue,
            AVG(Net_Income) as Avg_Net_Income,
            AVG(Profit_Margin) as Avg_Profit_Margin,
            AVG(ROE) as Avg_ROE,
            AVG(ROA) as Avg_ROA,
            AVG(Debt_to_Equity) as Avg_Debt_to_Equity,
            AVG(Current_Ratio) as Avg_Current_Ratio
        FROM `{CONFIG['project-name']}.{dataset_id}.stock_fundamentals`
        GROUP BY Ticker, Company_Name, Year
        ORDER BY Year DESC, Ticker
        """,
        "monthly_price_stats": f"""
        CREATE OR REPLACE VIEW `{CONFIG['project-name']}.{dataset_id}.monthly_price_stats` AS
        SELECT 
            Ticker, Year, Month,
            COUNT(*) as Trading_Days,
            AVG(Close) as Avg_Close_Price,
            MIN(Low) as Month_Low,
            MAX(High) as Month_High,
            SUM(Volume_Millions) as Total_Volume_Millions,
            AVG(Daily_Return) as Avg_Daily_Return
        FROM `{CONFIG['project-name']}.{dataset_id}.stock_prices`
        GROUP BY Ticker, Year, Month
        ORDER BY Year DESC, Month DESC, Ticker
        """,
        "top_performers": f"""
        CREATE OR REPLACE VIEW `{CONFIG['project-name']}.{dataset_id}.top_performers` AS
        WITH latest_year AS (
            SELECT MAX(Year) as max_year FROM `{CONFIG['project-name']}.{dataset_id}.stock_fundamentals`
        )
        SELECT 
            f.Ticker, f.Company_Name, f.Year, f.Revenue, f.Net_Income,
            f.Profit_Margin, f.ROE, f.ROA
        FROM `{CONFIG['project-name']}.{dataset_id}.stock_fundamentals` f
        CROSS JOIN latest_year ly
        WHERE f.Year = ly.max_year AND f.Revenue > 1000000000 AND f.Profit_Margin > 10
        ORDER BY f.Revenue DESC LIMIT 50
        """
    }
    for view_name, query in view_queries.items():
        query_job = client.query(query)
        query_job.result()
        print(f"✓ Created view: {view_name}")
    return list(view_queries.keys())

@task(name="validate_data_quality", retries=1, cache_policy=NO_CACHE)
def validate_data(client: bigquery.Client):
    print("Validating data quality...")
    dataset_id = CONFIG['dataset-name']
    checks = [
        {"name": "Fundamentals row count", "query": f"SELECT COUNT(*) as count FROM `{CONFIG['project-name']}.{dataset_id}.stock_fundamentals`"},
        {"name": "Prices row count", "query": f"SELECT COUNT(*) as count FROM `{CONFIG['project-name']}.{dataset_id}.stock_prices`"},
        {"name": "Unique tickers in fundamentals", "query": f"SELECT COUNT(DISTINCT Ticker) as count FROM `{CONFIG['project-name']}.{dataset_id}.stock_fundamentals`"},
        {"name": "Unique tickers in prices", "query": f"SELECT COUNT(DISTINCT Ticker) as count FROM `{CONFIG['project-name']}.{dataset_id}.stock_prices`"}
    ]
    results = {}
    for check in checks:
        query_job = client.query(check["query"])
        result = list(query_job.result())[0]
        results[check["name"]] = result['count']
        print(f"  {check['name']}: {result['count']:,}")
    return results

@flow(name="load_to_bigquery", log_prints=True)
def load_flow():
    print("Starting BigQuery load flow")
    print(f"Project: {CONFIG['project-name']}")
    print(f"Dataset: {CONFIG['dataset-name']}")
    client = create_client()
    setup_environment(client)
    register_external_tables(client)
    fundamentals_table = create_fundamentals_table(client)
    prices_table = create_prices_table(client)
    views = create_aggregated_views(client)
    validation_results = validate_data(client)
    print("\n✓ Load flow completed successfully")
    print(f"\nMaterialized Tables:")
    print(f"  - {fundamentals_table}")
    print(f"  - {prices_table}")
    print(f"\nAnalysis Views:")
    for view in views:
        print(f"  - {view}")
    print(f"\nValidation Summary:")
    for check, count in validation_results.items():
        print(f"  {check}: {count:,}")

if __name__ == "__main__":
    load_flow()
