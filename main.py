import dlt
from pathlib import Path

@dlt.source
def parquet_files_source(folder_path: str):
    """
    DLT source that reads parquet files from folder
    """

    @dlt.resource(write_disposition="append")
        """
        DLT Resource that yields data from paruqet files
        """

        folder = Path(folder_path)

        parquet_files = list(folder.glob("*.parquet"))

        if not parquet_files:
            print(f"No parquet files found in folder {folder_path}")
            return

        