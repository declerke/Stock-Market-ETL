import subprocess
from prefect import flow, task
from utils.config import CONFIG, CREDENTIALS_PATH

@task(name="check_docker_running", retries=0)
def check_docker():
    try:
        result = subprocess.run(
            ["docker", "ps"],
            capture_output=True,
            text=True,
            check=True
        )
        print("✓ Docker is running")
        return True
    except subprocess.CalledProcessError:
        print("✗ Docker is not running. Please start Docker and try again.")
        raise RuntimeError("Docker is not running")

@task(name="start_spark_cluster", retries=1)
def start_spark_cluster():
    print("Starting Spark cluster with docker-compose...")
    try:
        subprocess.run(
            ["docker-compose", "up", "-d"],
            check=True,
            capture_output=True,
            text=True
        )
        print("✓ Spark cluster started")
        import time
        print("Waiting 30 seconds for cluster initialization...")
        time.sleep(30)
    except subprocess.CalledProcessError as e:
        print(f"Error starting Spark cluster: {e.stderr}")
        raise

@task(name="transform_fundamentals", retries=1)
def transform_fundamentals():
    print("Transforming fundamentals data with Spark...")
    cmd = [
        "docker", "exec", "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "/opt/spark-apps/transform_stock_data.py",
        CONFIG['project-name'],
        "/opt/spark-apps/gcp_credentials.json",
        CONFIG['bucket-name'],
        "fundamentals"
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        print(result.stdout)
        print("✓ Fundamentals transformation completed")
    except subprocess.CalledProcessError as e:
        print(f"Error during transformation: {e.stderr}")
        print(f"Stdout: {e.stdout}")
        raise

@task(name="transform_prices", retries=1)
def transform_prices():
    print("Transforming price data with Spark...")
    cmd = [
        "docker", "exec", "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "/opt/spark-apps/transform_stock_data.py",
        CONFIG['project-name'],
        "/opt/spark-apps/gcp_credentials.json",
        CONFIG['bucket-name'],
        "prices"
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        print(result.stdout)
        print("✓ Prices transformation completed")
    except subprocess.CalledProcessError as e:
        print(f"Error during transformation: {e.stderr}")
        print(f"Stdout: {e.stdout}")
        raise

@task(name="stop_spark_cluster")
def stop_spark_cluster():
    print("Stopping Spark cluster...")
    try:
        subprocess.run(
            ["docker-compose", "down"],
            check=True,
            capture_output=True,
            text=True
        )
        print("✓ Spark cluster stopped")
    except subprocess.CalledProcessError as e:
        print(f"Warning: Could not stop Spark cluster: {e.stderr}")

@flow(name="transform_stock_data", log_prints=True)
def transform_flow():
    print("Starting Spark transformation flow")
    print(f"Project: {CONFIG['project-name']}")
    print(f"Bucket: {CONFIG['bucket-name']}")
    check_docker()
    start_spark_cluster()
    try:
        transform_fundamentals()
        transform_prices()
        print("\n✓ Transform flow completed successfully")
        print("  - Fundamentals transformed and partitioned by Year/Quarter")
        print("  - Prices transformed and partitioned by Year/Month")
    finally:
        stop_spark_cluster()

if __name__ == "__main__":
    transform_flow()
