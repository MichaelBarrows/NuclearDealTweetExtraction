import helpers
import pandas as pd
import dataset as ds
from termcolor import colored

def term_tagger (list_of_terms):
    terms_processed = []
    for items in list_of_terms:
        items = items.lower()
        items = items.split()
        for item in items:
            if item not in terms_processed:
                terms_processed.append(item)
    return terms_processed

def single_list_generator():
    list_of_terms = []
    df = helpers.load_dataset(ds.output_data + "keywords/original_keywords.csv")
    for index, row in df.iterrows():
        list_of_terms.append(row.keywords)

    tagged_terms = term_tagger(list_of_terms)
    list_df = pd.DataFrame(tagged_terms, columns=["keyword"])
    output_dir = ds.output_data + "keywords/"
    helpers.path_checker(output_dir)
    output_file = output_dir + "keywords_single_list.csv"
    helpers.dataframe_to_csv(list_df, output_file)
    return output_file

def run():
    print("\33[93m- keyword_single_list_creator.py\33[0m")
    print("  - Generating list of single keywords")
    output_file = single_list_generator()
    print("    - Output files:")
    print("      - " + output_file + "\n")
    print("\33[92m  - Complete \33[0m \n")
