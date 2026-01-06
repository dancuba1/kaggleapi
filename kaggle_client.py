
from kaggle.api.kaggle_api_extended import KaggleApi
from pathlib import Path
import zipfile
import hashlib

import json


base_url = "https://www.kaggle.com/api/v1"
owner_slug = "datasnaek"
dataset_slug = "youtube-new"
dataset_version = "115"
csv_file_name = "GBvideos.csv"
json_file_name = "GB_category_id.json"

RAW_DIR = Path("data/raw")
ZIP_PATH = RAW_DIR / "youtube-new.zip"  # the downloaded zip

url = f"{base_url}/datasets/download/{owner_slug}/{dataset_slug}?datasetVersionNumber={dataset_version}"

VERSION_FILE = Path("data/raw/dataset_version.txt")
DATASET_REF = "datasnaek/youtube-new"

 #Set for O(1) lookups
FILES_TO_KEEP = {"GBvideos.csv", "GB_category_id.json"}
#hash file to track dataset changes
HASH_FILE = RAW_DIR / "dataset_hash.txt"

class DatasetDownloadError(Exception):
    pass

def download_dataset():
    # Step 1: Authenticate
    try:
        api = KaggleApi()
        api.authenticate()
        print("Authenticated with Kaggle API")
    except Exception as e:
        raise DatasetDownloadError("Failed to authenticate with Kaggle") from e

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Step 2: Download the dataset
    print("Downloading dataset...")
    try:
        api.dataset_download_files("datasnaek/youtube-new", path=RAW_DIR)
    except Exception as e:
        raise DatasetDownloadError("Failed to download dataset from Kaggle") from e

    # Step 3: Find downloaded zip
    zip_files = list(RAW_DIR.glob("*.zip"))
    if not zip_files:
        raise DatasetDownloadError("No zip file downloaded")
    zip_path = zip_files[0]

    # Step 4: Compute hash and check if extraction needed
    remote_hash = compute_file_hash(zip_path)
    local_hash = get_local_hash()

    if local_hash == remote_hash:
        print("Dataset unchanged. Skipping extraction.")
        zip_path.unlink()  # delete zip
        return

    print("Dataset changed. Extracting needed files...")
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_contents = set(zip_ref.namelist())
            missing_files = FILES_TO_KEEP - zip_contents
            if missing_files:
                raise DatasetDownloadError(f"Missing files in zip: {missing_files}")
            for f in FILES_TO_KEEP:
                zip_ref.extract(f, RAW_DIR)
    except zipfile.BadZipFile as e:
        raise DatasetDownloadError("Corrupt zip file") from e

    zip_path.unlink()
    print("Extraction complete, zip deleted.")

    # Step 6: Save new hash
    save_local_hash(remote_hash)
    print("Local dataset hash updated.")




def _download_zip(api: KaggleApi) -> Path:
    try:
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        api.dataset_download_files(f"{owner_slug}/{dataset_slug}", path=RAW_DIR)

        zip_files = list(RAW_DIR.glob("*.zip"))
        if not zip_files:
            raise DatasetDownloadError("Download completed but no zip file found")

        return zip_files[0]

    except Exception as e:
        raise DatasetDownloadError("Failed to download dataset") from e




def _extract_needed_files(zip_path: Path) -> None:
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            print(zip_ref.namelist())

            zip_contents = set(zip_ref.namelist())
            missing_files = FILES_TO_KEEP - zip_contents

            if missing_files:
                raise DatasetDownloadError(f"Missing files in zip: {missing_files}")

            for f in FILES_TO_KEEP:
                zip_ref.extract(f, RAW_DIR)

        zip_path.unlink()

    except zipfile.BadZipFile as e:
        raise DatasetDownloadError("Corrupt zip file") from e



def compute_file_hash(file_path: Path, algorithm="md5") -> str:
    h = hashlib.new(algorithm)
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def get_local_hash() -> str | None:
    if HASH_FILE.exists():
        return HASH_FILE.read_text().strip()
    return None

def save_local_hash(file_hash: str) -> None:
    HASH_FILE.parent.mkdir(parents=True, exist_ok=True)
    HASH_FILE.write_text(file_hash)