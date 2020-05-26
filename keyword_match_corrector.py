import helpers
import dataset as ds
import pandas as pd

def keyword_splitter_to_list(keyword):
    return keyword.split()

def load_tagged_keywords():
    tagged_keywords = helpers.load_dataset(ds.output_data + "keywords/keywords_tagged.csv")
    tagged_keywords["split"] = ""
    for index, row in tagged_keywords.iterrows():
        tagged_keywords.split.at[index] = keyword_splitter_to_list(row.term)
    return tagged_keywords

def find_match_files():
    matches_data_files = {}
    matches_storage_path = ds.output_data + "individual_keyword_matches/"
    dataset_paths = helpers.path_fetcher(matches_storage_path)
    for path in dataset_paths:
        files = helpers.path_fetcher(matches_storage_path + path)
        matches_data_files[path] = []
        for file in files:
            f_path = matches_storage_path + path + "/" + file
            matches_data_files[path].append(f_path)
    return matches_data_files

def load_match_file(file):
    return helpers.load_dataset(file)

def comparison(matches, keywords):
    counter = 0
    matched_keywords = []
    for keyword in keywords:
        for match in matches:
            if keyword == match:
                counter += 1
                matched_keywords.append(keyword)
    if len(keywords) == counter:
        return True
    return False

def remove_duplicates(match_list):
    new_match_list = []
    for word in match_list:
        if word not in new_match_list:
            new_match_list.append(word)
    return new_match_list

def processing():
    tagged_keywords = load_tagged_keywords()
    match_files = find_match_files()
    files_created = []
    for dataset in match_files:
        for file in match_files[dataset]:
            print("      - " + file)
            new_store = {}
            generic_matches = {}
            specific_matches = {}
            matches_df = load_match_file(file)
            for match_index, match_row in matches_df.iterrows():
                generic_counter = 0
                specific_counter = 0
                tempstore = []
                match_row.matches = match_row.matches.strip("''][").split("', '")
                match_row.matches = remove_duplicates(match_row.matches)
                matches_df.matches.at[match_index] = match_row.matches
                for keyword_index, keyword_row in tagged_keywords.iterrows():
                    if comparison(match_row.matches, keyword_row['split']):
                        if keyword_row.tag =="generic":
                            generic_counter += 1
                        if keyword_row.tag == "specific":
                            specific_counter += 1
                        tempstore.append(keyword_row.term)

                if specific_counter != 0:
                    specific_matches[match_row.tweet_id] = tempstore
                else:
                    if generic_counter != 0:
                        generic_matches[match_row.tweet_id] = tempstore
            generic_data_list = []
            specific_data_list = []

            for tweet_id in generic_matches:
                generic_data_list.append([tweet_id, generic_matches[tweet_id]])
            for tweet_id in specific_matches:
                specific_data_list.append([tweet_id, specific_matches[tweet_id]])

            generic_file_path = ds.output_data + "actual_keyword_matches/generic/" + file.split("/")[-2] + "/"
            helpers.path_checker(generic_file_path)
            generic_file_name = generic_file_path + file.split("/")[-1]
            helpers.data_to_file_two_values(generic_data_list, '"tweet_id","matches"', generic_file_name)
            files_created.append(generic_file_name)

            specific_file_path = ds.output_data + "actual_keyword_matches/specific/" + file.split("/")[-2] + "/"
            helpers.path_checker(specific_file_path)
            specific_file_name = specific_file_path + file.split("/")[-1]
            helpers.data_to_file_two_values(specific_data_list, '"tweet_id","matches"', specific_file_name)
            files_created.append(specific_file_name)
            file_path = ds.output_data + "single_keyword_matches_dup_removed/" + file.split("/")[-2] + "/"
            helpers.path_checker(file_path)
            file_name = file_path + file.split("/")[-1]
            helpers.dataframe_to_csv(matches_df, file_name)
            files_created.append(file_name)
    return files_created
    # print(new_store)


def run():
    print("\33[93m- keyword_match_corrector.py\33[0m")
    print("  - creating actual matches and removing duplicates")
    print("    - Processing files:")
    output_files = processing()
    print("\n    - Output files:")
    for file in output_files:
        print("      - " + file)
    print("\n\33[92m  - Complete \33[0m \n")
