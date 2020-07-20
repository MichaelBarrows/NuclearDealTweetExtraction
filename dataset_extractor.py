import helpers
import dataset as ds
import pandas as pd
import os
import datetime as dt

# match_extractor()
# parameters:
#   matches : DataFrame - dataframe containing tweet ids and keywords matched
#   tweets : DataFrame - dataframe containing all of the english tweets (original)
#   storage : DataFrame - empty dataframe for storing extracted tweets
#   file : string - filename that the tweets originate from
#   d_type : string - either contains "generic" or "specific" - used for console
#       output
# returns:
#   storage : DataFrame - modified dataframe with extracted tweets
# description:
#   This function loops over each match and extracts the original tweet into a
#       dataframe, adding it to a list of dataframes to be merged. The tweet
#       also has a filename added to the filename column and the matched
#       keywords.
def match_extractor(matches, tweets, storage, file, d_type):
    temp_store = []
    counter = 0
    temp_store.append(storage)
    for match_index, match_row in matches.iterrows():
        temp_row = tweets[tweets.tweetid == match_row.tweet_id]
        for index, row in temp_row.iterrows():
            temp_row.matches.at[index] = str(match_row.matches)
            temp_row.source_file.at[index] = file
        temp_store.append(temp_row)
        counter += 1
        print("        -", str(counter), "/", str(len(matches)), d_type, file.split("/")[1])
    storage = pd.concat(temp_store)
    return storage

# tweet_extractor()
# parameters:
#   None
# returns:
#   files_created_generic : list - list of file names for generic matches
#   files_created_specific : list - list of file names for specific matches
# description:
#   This function creates additional columns on the dataframe containing the
#       original tweets (does not modify actual file), calls the match_extractor()
#       function to extract the tweet and stores the extracted tweets in a new
#       CSV file.
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
        columns.append("month")
        columns.append("year")
        df["matches"] = ""
        df["source_file"] = ""
        df["tweet_time"] = df["tweet_time"].astype("datetime64")
        df["month"] = df["tweet_time"].dt.month
        df["year"] = df["tweet_time"].dt.year
        specific_tweets, generic_tweets = pd.DataFrame(columns = columns), pd.DataFrame(columns = columns)
        specific_tweets = match_extractor(specific_df, df, specific_tweets, file, "specific")
        generic_tweets = match_extractor(generic_df, df, generic_tweets, file, "generic")
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

# run()
# parameters:
#   None
# returns:
#   None
# description:
#   This function is called to run the code in this file (by calling the
#       tweet_extractor() function) and generates console output detailing the
#       purpose of the file, its progress and output files created.
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
