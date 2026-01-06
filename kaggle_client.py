from kaggle.api.kaggle_api_extended import KaggleApi
from pathlib import Path
import zipfile
import hashlib

# Dataset constants
OWNER_SLUG = "datasnaek"
DATASET_SLUG = "youtube-new"
CSV_FILE_NAME = "GBvideos.csv"
JSON_FILE_NAME = "GB_category_id.json"
FILES_TO_KEEP = {CSV_FILE_NAME, JSON_FILE_NAME}

RAW_DIR = Path("data/raw")
ZIP_PATH = RAW_DIR / "youtube-new.zip"
HASH_FILE = RAW_DIR / "dataset_hash.txt"

class DatasetDownloadError(Exception):
    pass


# ----------------------------
# Public function
# ----------------------------
def download_dataset() -> None:
    """Authenticate, download, extract needed files, and update hash."""
    api = _authenticate_kaggle()
    zip_path = _download_if_needed(api)
    if zip_path is None:
        print("Dataset unchanged. Skipping download and extraction.")
        return
    _extract_needed_files(zip_path)
    print("Pipeline completed successfully.")



def _authenticate_kaggle() -> KaggleApi:
    try:
        api = KaggleApi()
        api.authenticate()
        print("Authenticated with Kaggle API")
        return api
    except Exception as e:
        raise DatasetDownloadError("Failed to authenticate with Kaggle") from e


def _download_if_needed(api: KaggleApi) -> Path | None:
    """Download dataset zip only if hash differs. Returns zip_path or None if unchanged."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print("Downloading dataset...")
    try:
        api.dataset_download_files(f"{OWNER_SLUG}/{DATASET_SLUG}", path=RAW_DIR)
    except Exception as e:
        raise DatasetDownloadError("Failed to download dataset from Kaggle") from e

    zip_files = list(RAW_DIR.glob("*.zip"))
    if not zip_files:
        raise DatasetDownloadError("No zip file downloaded")
    zip_path = zip_files[0]

    remote_hash = compute_file_hash(zip_path)
    local_hash = get_local_hash()
    if local_hash == remote_hash:
        zip_path.unlink()  # delete zip since nothing changed
        return None

    print("Dataset changed. Proceeding with extraction.")
    save_local_hash(remote_hash)
    return zip_path


def _extract_needed_files(zip_path: Path) -> None:
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_contents = set(zip_ref.namelist())
            missing_files = FILES_TO_KEEP - zip_contents
            if missing_files:
                raise DatasetDownloadError(f"Missing files in zip: {missing_files}")
            for f in FILES_TO_KEEP:
                zip_ref.extract(f, RAW_DIR)
        zip_path.unlink()
        print("Extraction complete, zip deleted.")
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
    print(f"Saved local dataset hash: {file_hash}")
