from kaggle_client import download_dataset, DatasetDownloadError
from transform import transform_and_save_data, DatasetTransformError
from graph import plot_engagement_graph


def main():
    try:
        download_dataset()
        engagement_df = transform_and_save_data()
        engagement_by_category = engagement_df.set_index('category_name')['engagement_score']
        plot_engagement_graph(engagement_by_category)
        print("Pipeline completed successfully")
    except DatasetDownloadError as e:
        print(f"Dataset error: {e}")
    except DatasetTransformError as e:
        print(f"Transform error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()

