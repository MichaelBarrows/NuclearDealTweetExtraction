import pandas as pd
import numpy as np
import operator
import dataset
import csv
import os

def stats_write_to_file(headers, name, value, percentage, filename):
    with open(dataset.dataset + "/" + filename, mode='w') as file_writer:
        file_writer.write(headers + "\n")
        file_writer.write('"' + str(name) + '", "' + str(value) + '", "' + str(percentage) + '%"' + "\n")
    return

def stats_append_to_file(name, value, percentage, filename):
    with open(dataset.dataset + "/" + filename, mode='a') as file_writer:
        file_writer.write('"' + str(name) + '", "' + str(value) + '", "' + str(percentage) + '%"' + "\n")
    return

def data_to_file_two_values(data, headings, filename):
    with open(filename, mode='w') as file_writer:
        file_writer.write(headings + "\n")
        for row in data:
            file_writer.write(str(row[0]) + ',"' + str(row[1]) + '"' + "\n")
    return

def data_to_file(data, headings, filename):
    with open(filename, mode='w') as file_writer:
        file_writer.write(headings + "\n")
        for row in data:
            file_writer.write(str(row[0]) + ',' + str(row[1]) + ',"' + str(row[2]) + '"' + "\n")
    return

def dataframe_to_csv(pd_df, filename):
    return pd_df.to_csv(filename, index=False, quoting=csv.QUOTE_NONNUMERIC)

def load_dataset(input_csv):
    df = pd.read_csv(input_csv, header=0, low_memory=False)
    return df

def percentage(part, whole):
    return round(100 * float(part)/float(whole), 2)

def sort_dict(dictionary):
    return dict(sorted(dictionary.items(),
                        key=operator.itemgetter(1),
                        reverse=True))

def output_to_text_file(data, filename, mode):
    with open(filename, mode) as file_writer:
        file_writer.write(data)

def file_reader_to_list(file):
    if os.path.exists(file) == False:
        return []
    list = []
    with open(file) as file_reader:
        for line in file_reader:
            list.append(line.strip('\n'))
    return list

def path_checker(path):
    path = path.split("/")
    path_so_far = ""
    for dir in path:
        path_so_far += "/" + dir
        if os.path.exists(path_so_far) == False:
            path_creator(path_so_far)

def path_creator(path):
    os.mkdir(path)

def path_fetcher(path):
    return os.listdir(path)

def get_dataset_file_paths(root_path):
    files = []
    datasets_in_path = os.listdir(root_path)
    for ds in datasets_in_path:
        ds_path = root_path + "/" + ds
        files_in_dataset_path = os.listdir(ds_path)
        for file in files_in_dataset_path:
            files.append(ds_path + "/" + file)
    return files
