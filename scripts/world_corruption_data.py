import pandas as pd
from pathlib import Path
from typing import List, Dict, TypedDict, Optional, Any
import logging

#current_directory = Path.cwd().parent [is a valid alternative]
current_dir = Path(__file__).parent

file_name = "corruption.csv"
file_path_obj = str((current_dir.parent / file_name).resolve())

def csv_reader(file_path: str) -> pd.DataFrame:

    df = pd.read_csv(file_path)
    csv_reader.__doc__ = f" the dataframe is {len(df)} rows."
    return df


if __name__ == "__main__":
    print(csv_reader(file_path_obj))
    print(csv_reader.__doc__)
    
  
    