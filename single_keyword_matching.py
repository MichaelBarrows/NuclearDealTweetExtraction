import pandas as pd
import helpers
import dataset as ds
import re
import os

# list_creator()
# parameters:
#   data : DataFrame - raw keywords from CSV file
# returns:
#   list : list - list containing the keywords
# description:
#   This function extracts the keywords column from the dataframe and returns it
#       as a list.
def list_creator (data):
    list = []
    for index, row in data.iterrows():
        list.append(row.keyword)
    return list

# clean_tweet()
# parameters:
#   current_tweet_text : string - the current raw text of the tweet
# returns:
#   current_tweet_text : string - the cleaned tweet text
# description:
#   This function modifies the text of the tweet by removing symbols, reserved
#       words and other characters that can hinder processing.
def clean_tweet(current_tweet_text):
    current_tweet_text = re.sub(r'&amp|amp|&amp;', '', current_tweet_text)
    current_tweet_text = re.sub(r'via\s[.+]\s', '', current_tweet_text)
    current_tweet_text = re.sub(r'\b@\s[.+]\s', '', current_tweet_text)
    current_tweet_text = re.sub(r'\b@[.+]\s', '', current_tweet_text)
    current_tweet_text = re.sub(r'#', '', current_tweet_text)
    current_tweet_text = re.sub(r'http.+\s', '', current_tweet_text)
    current_tweet_text = re.sub(r'https.+\s', '', current_tweet_text)
    current_tweet_text = re.sub(r'http', '', current_tweet_text)
    current_tweet_text = re.sub(r'https', '', current_tweet_text)
    current_tweet_text = re.sub(r"£", '', current_tweet_text)
    current_tweet_text = re.sub(r"%", '', current_tweet_text)
    current_tweet_text = re.sub(r"'", '', current_tweet_text)
    current_tweet_text = re.sub(r"‘", '', current_tweet_text)
    current_tweet_text = re.sub(r"’", '', current_tweet_text)
    current_tweet_text = re.sub(r",", '', current_tweet_text)
    current_tweet_text = re.sub(r"/", '', current_tweet_text)
    current_tweet_text = re.sub(r"\\", '', current_tweet_text)
    current_tweet_text = re.sub(r"\.", '', current_tweet_text)
    current_tweet_text = re.sub(r"!", '', current_tweet_text)
    current_tweet_text = re.sub(r"\?", '', current_tweet_text)
    current_tweet_text = re.sub(r'"', '', current_tweet_text)
    current_tweet_text = re.sub(r':', '', current_tweet_text)
    current_tweet_text = re.sub(r';', '', current_tweet_text)
    current_tweet_text = re.sub(r'|', '', current_tweet_text)
    current_tweet_text = re.sub(r'–', '', current_tweet_text)
    current_tweet_text = re.sub(r'…', '', current_tweet_text)
    current_tweet_text = re.sub(r'', '', current_tweet_text)
    current_tweet_text = re.sub(r'\(', '', current_tweet_text)
    current_tweet_text = re.sub(r'\)', '', current_tweet_text)
    current_tweet_text = re.sub(r'\[', '', current_tweet_text)
    current_tweet_text = re.sub(r'\]', '', current_tweet_text)
    current_tweet_text = re.sub(r'\$', '', current_tweet_text)
    current_tweet_text = re.sub(r'\€', '', current_tweet_text)
    current_tweet_text = re.sub(r'ğ', 'g', current_tweet_text)
    current_tweet_text = re.sub(r'`', '', current_tweet_text)
    current_tweet_text = re.sub(r'“', '', current_tweet_text)
    current_tweet_text = re.sub(r'”', '', current_tweet_text)
    # current_tweet_text = re.sub(r'\s-\s', ' ', current_tweet_text)
    current_tweet_text = re.sub(r'\|', '', current_tweet_text)
    current_tweet_text = re.sub(r"#", '', current_tweet_text)
    return current_tweet_text

# check_keyword()
# parameters:
#   tweet_text : string - the text of the tweet to check against the keywords
#   keywords_list : list - list containing the keywords to be checked against
# returns:
#   tempstore : list - list of keywords that were matched
# description:
#   This function checks each word in the tweet against each keyword in the list
#       for potential matches, and returns a list of matched keywords.
def check_keyword(tweet_text, keywords_list):
    tempstore = []
    tweet_text = tweet_text.lower().split()
    for tweet_word in tweet_text:
        for keyword in keywords_list:
            if keyword == tweet_word:
                tempstore.append(keyword)
    return tempstore

# processing()
# parameters:
#   None
# returns:
#   file_paths : list - list containing the paths of each of the files created
# description:
#   This function iterates over each of the file paths, imports the datasets,
#       loops over each english tweet in each data file and calls the
#       check_keyword() function to perform the match checking. The matches are
#       stored to a file.
def processing():
    keyword_list = helpers.load_dataset(ds.output_data + "keywords/keywords_single_list.csv")
    store = {}
    keyword_list = list_creator(keyword_list)
    file_paths = []

    for df in ds.all_datasets:
        print("    - Processing", df)
        f_name = df
        store[f_name] = {}
        df = helpers.load_dataset(ds.dataset + df)
        df = df[df.tweet_language == "en"]
        for index, row in df.iterrows():
            matches = check_keyword(clean_tweet(row.tweet_text), keyword_list)
            if len(matches) != 0:
                store[f_name][row.tweetid] = matches
    # # storage
    matches_counter = 0
    for f_name in store:
        data_list = []
        filename = f_name.split("/")
        dataset = filename[0]
        filename = filename[1]
        path = ds.output_data + "individual_keyword_matches/"
        dataset_path = path + dataset + "/"
        helpers.path_checker(dataset_path)
        file_path = dataset_path + filename
        for item in store[f_name]:
            data_list.append([item, store[f_name][item]])
            matches_counter += 1
        helpers.data_to_file_two_values(data_list, '"tweet_id","matches"', file_path)
        file_paths.append(file_path)
    return file_paths

# run()
# parameters:
#   None
# returns:
#   None
# description:
#   This function is called to run the code in this file (by calling the
#       processing() function). It generates console output to inform the user
#       of its purpose, progress and output files.
def run():
    print("\33[93m- single_keyword_matching.py\33[0m")
    print("  - Identifying tweets matching single keyword list")
    output_files = processing()
    print("    - Output files:")
    for file in output_files:
        print("      - " + file)
    print("\33[92m  - Complete \33[0m \n")
