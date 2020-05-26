import helpers
import dataset as ds
import pandas as pd
import os
import datetime as dt

def match_checker(matches, tweets, storage, tweet_ids, file, d_type):
    for index, row in tweets.iterrows():
        if row.tweetid in tweet_ids:
            for matches_index, matches_row in matches.iterrows():
                if row.tweetid == matches_row.tweet_id:
                    tweets.matches.at[index] = str(matches_row.matches)
                    tweets.source_file.at[index] = file
                    temp_df = tweets[tweets.tweetid == row.tweetid]
                    storage = pd.concat([storage, temp_df])
                    if len(storage) % 100 == 0:
                        print("        -", str(len(storage)), "/", str(len(matches)), d_type, file.split("/")[1])
    return storage

def tweet_extractor():
    files_created_generic = []
    files_created_specific = []
    for file in ds.all_datasets:
        generic_df = helpers.load_dataset(ds.output_data + "actual_keyword_matches/generic/" + file)
        specific_df = helpers.load_dataset(ds.output_data + "actual_keyword_matches/specific/" + file)
        print("      - loading data", file)
        df = helpers.load_dataset(ds.dataset + file)
        df = df[df.tweet_language == "en"]
        columns = []
        for h in df.head():
            columns.append(h)
        columns.append("matches")
        columns.append("source_file")
        columns.append("year")
        df["matches"] = ""
        df["source_file"] = ""
        df["tweet_time"] = df["tweet_time"].astype("datetime64")
        df["year"] = df["tweet_time"].dt.year
        specific_tweets, generic_tweets = pd.DataFrame(columns = columns), pd.DataFrame(columns = columns)
        specific_tweets = match_checker(specific_df, df, specific_tweets, specific_df['tweet_id'].tolist(), file, "specific")
        generic_tweets = match_checker(generic_df, df, generic_tweets, generic_df['tweet_id'].tolist(), file, "generic")
        output_data_path = ds.output_data + "first_dataset_extraction/"
        dataset = file.split("/")[0]
        filename = file.split("/")[1]

        specific_path = output_data_path + "specific/" + dataset + "/"
        helpers.path_checker(specific_path)
        helpers.dataframe_to_csv(specific_tweets, specific_path + filename)
        files_created_specific.append(specific_path + filename)

        generic_path = output_data_path + "generic/" + dataset + "/"
        helpers.path_checker(generic_path)
        helpers.dataframe_to_csv(generic_tweets, generic_path + filename)
        files_created_generic.append(generic_path + filename)
    return files_created_generic, files_created_specific

def run():
        print("\33[93m- dataset_extractor.py\33[0m")
        print("  - extracting tweets to new dataset according to match files")
        print("    - Processing files:")
        output_files_generic, output_files_specific = tweet_extractor()
        print("\n    - Output files (Generic):")
        for file in output_files_generic:
            print("      - " + file)
        print("\n    - Output files (Specific):")
        for file in output_files_specific:
            print("      - " + file)
        print("\n\33[92m  - Complete \33[0m \n")
