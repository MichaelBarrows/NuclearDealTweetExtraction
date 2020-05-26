import helpers
import pandas as pd
import dataset as ds
from termcolor import colored

list_of_terms = []

def term_tagger (list_of_terms):
    tagged_terms = []
    counter = 0
    for item in list_of_terms:
        item = item.lower()
        print("    - " + item)
        i = int(input("      - generic [0] / specific [1] / unknown [2]: "))
        if i == 0:
            # Yellow
            color = "\33[93m"
            i = "generic"
        elif i == 1:
            # Green
            color = "\33[92m"
            i = "specific"
        elif i == 2:
            # Red
            color = "\33[91m"
            i = "unknown"
        tagged_terms.append([item, i])
        print("      - " + color + i.upper(), "\33[0m - ", item, "\n")
    return tagged_terms

def tagged_keywords_generator():
    df = helpers.load_dataset(ds.output_data + "keywords/original_keywords.csv")
    for item in df.iterrows():
        list_of_terms.append(item[1][0])
    tagged_terms = term_tagger(list_of_terms)
    output_dir = ds.output_data + "keywords/"
    helpers.path_checker(output_dir)
    output_file = output_dir + "keywords_tagged.csv"
    helpers.data_to_file_two_values(tagged_terms, '"term","tag"', output_file)
    return output_file

def run():
    print("\33[93m- keyword_tagger.py\33[0m")
    print("  - Generating tagged keywords file")
    output_file = tagged_keywords_generator()
    print("    - Output files:")
    print("      - " + output_file + "\n")
    print("\33[92m  - Complete \33[0m \n")
