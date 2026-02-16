from prefect_gcp import GcpCredentials, GcsBucket
from utils.config import CONFIG, CREDENTIALS_PATH


def create_gcp_credentials_block():
    gcp_credentials = GcpCredentials(
        service_account_file=str(CREDENTIALS_PATH)
    )
    gcp_credentials.save("gcp-credentials", overwrite=True)
    print("✓ GCP Credentials block created")


def create_gcs_bucket_block():
    gcp_credentials = GcpCredentials.load("gcp-credentials")
    
    gcs_bucket = GcsBucket(
        bucket=CONFIG['bucket-name'],
        gcp_credentials=gcp_credentials
    )
    gcs_bucket.save("gcs-bucket", overwrite=True)
    print("✓ GCS Bucket block created")


def main():
    print("Creating Prefect GCP blocks...")
    print(f"Project: {CONFIG['project-name']}")
    print(f"Bucket: {CONFIG['bucket-name']}")
    print(f"Region: {CONFIG['region']}")
    
    create_gcp_credentials_block()
    create_gcs_bucket_block()
    
    print("\n✓ All blocks created successfully!")
    print("\nYou can now run the ETL flows:")
    print("  python flows/extract.py")
    print("  python flows/transform.py")
    print("  python flows/load.py")
    print("  python flows/orchestrate.py")


if __name__ == "__main__":
    main()