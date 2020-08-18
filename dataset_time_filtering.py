import helpers
import dataset as ds
import pandas as pd

# date_selection()
# parameters:
#   None
# returns:
#   output_files : list - List of created files
# description:
#   This function retrieves the merged datasets and filters them by year. The
#       filtered dataset is then output to a new CSV file.
def date_selection():
    output_files = []
    path = ds.output_data + "merged_dataset_extraction/"
    files = helpers.path_fetcher(path)
    for file in files:
        df = helpers.load_dataset(path + file)
        df_2013 = df[df.year == 2013]
        df_2013_8 = df_2013[df.month == 8]
        df_2013_9 = df_2013[df.month == 9]
        df_2013_10 = df_2013[df.month == 10]
        df_2013_11 = df_2013[df.month == 11]
        df_2013_12 = df_2013[df.month == 12]
        df = df[(df.year == 2014) | (df.year == 2015) | (df.year == 2016) | (df.year == 2017) | (df.year == 2018)]
        df = pd.concat([df_2013_8, df_2013_9, df_2013_10, df_2013_11, df_2013_12, df])
        storage_path = ds.output_data + "time_filtered_dataset_extraction/"
        helpers.path_checker(storage_path)
        helpers.dataframe_to_csv(df, storage_path + file)
        output_files.append(storage_path + file)
    return output_files

# run()
# parameters:
#   None
# returns:
#   None
# description:
#   This function is called to run the code in this file (by calling the
#       date_selection() function) and generates console output detailing the
#       purpose of the file, its progress and output files created.
def run():
    print("\33[93m- dataset_time_filtering.py\33[0m")
    print("  - extracting tweets for 2014/15/16/17/18")
    output_files = date_selection()
    print("    - Output files:")
    for output_file in output_files:
        print("      - " + output_file)
    print("\33[92m  - Complete \33[0m \n")
