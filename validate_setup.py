import sys
import subprocess
from pathlib import Path


def check_python_version():
    version = sys.version_info
    if version.major >= 3 and version.minor >= 9:
        print(f"✓ Python {version.major}.{version.minor}.{version.micro}")
        return True
    else:
        print(f"✗ Python version {version.major}.{version.minor} is too old. Need 3.9+")
        return False


def check_docker():
    try:
        result = subprocess.run(
            ["docker", "--version"],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"✓ {result.stdout.strip()}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("✗ Docker not found or not running")
        return False


def check_terraform():
    try:
        result = subprocess.run(
            ["terraform", "--version"],
            capture_output=True,
            text=True,
            check=True
        )
        version_line = result.stdout.split('\n')[0]
        print(f"✓ {version_line}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("✗ Terraform not found")
        return False


def check_git():
    try:
        result = subprocess.run(
            ["git", "--version"],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"✓ {result.stdout.strip()}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("✗ Git not found")
        return False


def check_config_file():
    pyenv_path = Path(".pyenv")
    if pyenv_path.exists():
        print("✓ .pyenv configuration file exists")
        return True
    else:
        print("✗ .pyenv file not found. Copy .pyenv.example and fill in your values.")
        return False


def check_credentials():
    creds_path = Path("gcp_credentials.json")
    if creds_path.exists():
        print("✓ gcp_credentials.json exists")
        return True
    else:
        print("✗ gcp_credentials.json not found. Download from GCP and save in project root.")
        return False


def check_virtual_environment():
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("✓ Virtual environment activated")
        return True
    else:
        print("⚠ Virtual environment not activated. Run: source venv/bin/activate")
        return False


def check_dependencies():
    required = ['prefect', 'simfin', 'google-cloud-storage', 'pyspark']
    missing = []
    
    for package in required:
        try:
            __import__(package.replace('-', '_'))
            print(f"✓ {package} installed")
        except ImportError:
            print(f"✗ {package} not installed")
            missing.append(package)
    
    if missing:
        print(f"\nInstall missing packages: pip install {' '.join(missing)}")
        return False
    return True


def main():
    print("="*60)
    print("ENVIRONMENT VALIDATION")
    print("="*60)
    
    checks = [
        ("Python Version", check_python_version),
        ("Docker", check_docker),
        ("Terraform", check_terraform),
        ("Git", check_git),
        ("Virtual Environment", check_virtual_environment),
        ("Configuration File", check_config_file),
        ("GCP Credentials", check_credentials),
        ("Python Dependencies", check_dependencies),
    ]
    
    results = []
    for name, check_func in checks:
        print(f"\n{name}:")
        results.append(check_func())
    
    print("\n" + "="*60)
    passed = sum(results)
    total = len(results)
    print(f"SUMMARY: {passed}/{total} checks passed")
    print("="*60)
    
    if passed == total:
        print("\n✓ Environment ready! You can now run:")
        print("  python flows/orchestrate.py")
    else:
        print("\n⚠ Fix the issues above before running the pipeline")
        print("\nSetup steps:")
        print("1. Create virtual environment: python -m venv venv")
        print("2. Activate: source venv/bin/activate")
        print("3. Install dependencies: pip install -r requirements.txt")
        print("4. Copy .pyenv.example to .pyenv and fill in values")
        print("5. Download GCP credentials to gcp_credentials.json")
        print("6. Run: python init_blocks.py")
        print("7. Provision infrastructure: cd terraform && terraform apply")


if __name__ == "__main__":
    main()