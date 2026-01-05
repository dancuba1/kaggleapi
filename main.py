# This is a very basic prompt file to get you started, feel free to amend as you please, though please use the dataset and file names provided
import kaggle
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi
from pathlib import Path
base_url = "https://www.kaggle.com/api/v1"
owner_slug = "datasnaek"
dataset_slug = "youtube-new"
dataset_version = "115"

csv_file_name = "GBvideos.csv"
json_file_name = "GB_category_id.json"
RAW_DIR = Path("data/raw")
ZIP_PATH = RAW_DIR / "youtube-new.zip"  # the downloaded zip



url = f"{base_url}/datasets/download/{owner_slug}/{dataset_slug}?datasetVersionNumber={dataset_version}"

def download_dataset():
    api = KaggleApi()
    api.authenticate()
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    api.dataset_download_files(owner_slug + "/" + dataset_slug, path=RAW_DIR)
    # Zip is dowloaded, so must unzip and keep only needed files
    keep_only_needed_files()

def keep_only_needed_files():
    
    #Set for O(1) lookups
    FILES_TO_KEEP = {"GBvideos.csv", "GB_category_id.json"}
    with zipfile.ZipFile(ZIP_PATH, "r") as zip_ref:
        for f in FILES_TO_KEEP:
            zip_ref.extract(f, RAW_DIR)
    ZIP_PATH.unlink()


def main():
    download_dataset()
    print(f"Dataset downloaded and extracted to {RAW_DIR}")

if __name__ == "__main__":
    main()

