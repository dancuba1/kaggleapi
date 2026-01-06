import pandas as pd
import json
from pathlib import Path

RAW_DIR = Path("data/raw")
CSV_FILE_PATH = RAW_DIR / "GBvideos.csv"
JSON_FILE_PATH = RAW_DIR / "GB_category_id.json"
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


class DatasetTransformError(Exception):
    pass

def transform_and_save_data():
    try:
        vids = pd.read_csv(CSV_FILE_PATH)

        with open(JSON_FILE_PATH, 'r') as f:
            categories = json.load(f)['items']

        category_mapping = {int(item['id']): item['snippet']['title'] for item in categories}

        vids['category_name'] = vids['category_id'].map(category_mapping)

        vids['engagement_score'] = vids['likes'] + vids['comment_count'] + vids['dislikes']
        vids['rating_ratio'] = vids['likes'] / (vids['likes'] + vids['dislikes']).replace({0: 1})

        engagement_by_category = (
            vids
            .groupby("category_name", as_index=False)[["engagement_score"]]
            .mean()
            .sort_values("engagement_score", ascending=False)
        )

        # Save to CSV
        output_path = PROCESSED_DIR / "engagement_by_category.csv"
        engagement_by_category.to_csv(output_path, index=False)

        return engagement_by_category

    except (FileNotFoundError, json.JSONDecodeError, KeyError, ZeroDivisionError) as e:
        raise DatasetTransformError(f"Data transformation failed: {e}") from e