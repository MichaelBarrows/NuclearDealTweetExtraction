import pandas as pd
import helpers
import dataset as ds
import re
import os

def list_creator (data):
    list = []
    for index, row in data.iterrows():
        list.append(row.keyword)
    return list

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

def check_keyword(tweet_text, keywords_list):
    tempstore = []
    tweet_text = tweet_text.lower().split()
    for tweet_word in tweet_text:
        for keyword in keywords_list:
            if keyword == tweet_word:
                tempstore.append(keyword)
    return tempstore

def check_tweet_list(tweet_id, tweet_text, f_name, keyword_list):
    matches = check_keyword(tweet_text, keyword_list)
    if len(matches) != 0:
        return matches
    else:
        return None


def processing():
    keyword_list = helpers.load_dataset(ds.output_data + "keywords/keywords_single_list.csv")
    store = {}
    keyword_list = list_creator(keyword_list)
    file_paths = []
# test text
    # tweet_text = "donald trump agreement bitch boy iran nuclear"
    # checker = check_tweet_list("010206971409", clean_tweet(tweet_text), "test", keyword_list)
    # print(checker)
    # file_paths.append('help.me')

    for df in ds.all_datasets:
        print("    - Processing", df)
        f_name = df
        store[f_name] = {}
        df = helpers.load_dataset(ds.dataset + df)
        df = df[df.tweet_language == "en"]
        for index, row in df.iterrows():
            matches = check_tweet_list(row.tweetid, clean_tweet(row.tweet_text), f_name, keyword_list)
            if matches != None:
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

def run():
    print("\33[93m- single_keyword_matching.py\33[0m")
    print("  - Identifying tweets matching single keyword list")
    output_files = processing()
    print("    - Output files:")
    for file in output_files:
        print("      - " + file)
    print("\33[92m  - Complete \33[0m \n")
