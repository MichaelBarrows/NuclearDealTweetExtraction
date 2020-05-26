import helpers
import dataset as ds
import pandas as pd

def date_selection():
    output_files = []
    path = ds.output_data + "merged_dataset_extraction/"
    files = helpers.path_fetcher(path)
    for file in files:
        df = helpers.load_dataset(path + file)
        df = df[(df.year == 2014) | (df.year == 2015) | (df.year == 2016) | (df.year == 2017) | (df.year == 2018)]
        storage_path = ds.output_data + "time_filtered_dataset_extraction/"
        helpers.path_checker(storage_path)
        helpers.dataframe_to_csv(df, storage_path + file)
        output_files.append(storage_path + file)
    return output_files

def run():
    print("\33[93m- dataset_time_filtering.py\33[0m")
    print("  - extracting tweets for 2014/15/16/17/18")
    output_files = date_selection()
    print("    - Output files:")
    for output_file in output_files:
        print("      - " + output_file)
    print("\33[92m  - Complete \33[0m \n")
