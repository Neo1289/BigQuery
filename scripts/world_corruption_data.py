import pandas as pd
from pathlib import Path

current_directory = Path.cwd()
file_name = "corruption.csv"

file_path_obj = str((current_directory / file_name).resolve())

df = pd.read_csv(file_path_obj)


if __name__ == "__main__":
    print(df.head())