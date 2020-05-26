import dataset as ds
import helpers
import pandas as pd

def merge(dataset_type):
    print("      - Processing " + dataset_type + " files:")
    for file in ds.all_datasets:
        print("        - " + file)
        file_path = file.split("/")
        f_name = ds.output_data + "first_dataset_extraction/" + dataset_type + "/" + file_path[0] + "/" + file_path[1]
        df = helpers.load_dataset(f_name)
        if file == ds.all_datasets[0]:
            merge_hold = df
        else:
            merge_hold = pd.concat([merge_hold, df], sort=False)
    output_path = ds.output_data + "merged_dataset_extraction/"
    helpers.path_checker(output_path)
    file_name = dataset_type + ".csv"
    helpers.dataframe_to_csv(merge_hold, output_path + file_name)
    return output_path + file_name


def run():
    print("\33[93m- dataset_merger.py\33[0m")
    print("  - merging individual datasets to create large dataset")
    print("    - Processing files:")
    specific_output_file = merge("specific")
    generic_output_file = merge("generic")
    print("    - Output files:")
    print("      - " + specific_output_file)
    print("      - " + generic_output_file)
    print("\n\33[92m  - Complete \33[0m \n")
