from prefect import flow
from flows.extract import extract_flow
from flows.transform import transform_flow
from flows.load import load_flow
from utils.config import CONFIG


@flow(name="stock_data_etl_pipeline", log_prints=True)
def orchestrate_pipeline():
    print("="*60)
    print("STOCK DATA ETL PIPELINE")
    print("="*60)
    print(f"Project: {CONFIG['project-name']}")
    print(f"Bucket: {CONFIG['bucket-name']}")
    print(f"Dataset: {CONFIG['dataset-name']}")
    print(f"Region: {CONFIG['region']}")
    print("="*60)
    
    print("\n[PHASE 1/3] EXTRACT - Fetching data from SimFin API")
    print("-"*60)
    extract_flow()
    
    print("\n[PHASE 2/3] TRANSFORM - Processing with Spark")
    print("-"*60)
    transform_flow()
    
    print("\n[PHASE 3/3] LOAD - Loading into BigQuery")
    print("-"*60)
    load_flow()
    
    print("\n" + "="*60)
    print("✓ COMPLETE ETL PIPELINE FINISHED SUCCESSFULLY")
    print("="*60)
    print("\nNext Steps:")
    print("1. Open Google Cloud Console → BigQuery")
    print(f"2. Navigate to dataset: {CONFIG['dataset-name']}")
    print("3. Query the materialized tables and views")
    print("4. Create a Looker Studio dashboard connected to BigQuery")
    print("\nSuggested BigQuery queries to try:")
    print(f"  SELECT * FROM `{CONFIG['project-name']}.{CONFIG['dataset-name']}.top_performers` LIMIT 10")
    print(f"  SELECT * FROM `{CONFIG['project-name']}.{CONFIG['dataset-name']}.annual_company_metrics` WHERE Year >= 2020")
    print(f"  SELECT * FROM `{CONFIG['project-name']}.{CONFIG['dataset-name']}.monthly_price_stats` WHERE Year = 2023")


if __name__ == "__main__":
    orchestrate_pipeline()