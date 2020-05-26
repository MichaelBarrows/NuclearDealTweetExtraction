import helpers
import pandas as pd
import dataset as ds

# word_extractor()
# parameters:
#   list_of_terms : list - containing original keywords/keyterms
# returns:
#   terms_processed : list - a list containing the individual words of all of
#       the keywords/phrases
# description:
#   This function extracts each individual word from a list of keywords/phrases.
#   Duplicates are prevented and the list of individual words is returned
def word_extractor(list_of_terms):
    terms_processed = []
    for items in list_of_terms:
        items = items.lower()
        items = items.split()
        for item in items:
            if item not in terms_processed:
                terms_processed.append(item)
    return terms_processed

# single_list_generator()
# parameters:
#   None
# returns:
#   output_file : string - path and filename for the file that the single list
#       of keywords was stored in
# description:
#   This function takes a dataset of keywords/phrases, transforms the DataFrame
#       to a list and sends the list to the word_extractor() function.
#   The returned list of single keywords is transformed into a DataFrame and
#       stored.
def single_list_generator():
    df = helpers.load_dataset(ds.output_data + "keywords/original_keywords.csv")
    list_of_terms = df.keywords.tolist()
    tagged_terms = word_extractor(list_of_terms)
    list_df = pd.DataFrame(tagged_terms, columns=["keyword"])
    output_dir = ds.output_data + "keywords/"
    helpers.path_checker(output_dir)
    output_file = output_dir + "keywords_single_list.csv"
    helpers.dataframe_to_csv(list_df, output_file)
    return output_file

# run()
# parameters:
#   None
# returns:
#   None
# description:
#   This function is called to run the file (by calling the
#       single_list_generator() function) and generates console output to inform
#       the user of its purpose, progress and output files (with file paths).
def run():
    print("\33[93m- keyword_single_list_creator.py\33[0m")
    print("  - Generating list of single keywords")
    output_file = single_list_generator()
    print("    - Output files:")
    print("      - " + output_file + "\n")
    print("\33[92m  - Complete \33[0m \n")
