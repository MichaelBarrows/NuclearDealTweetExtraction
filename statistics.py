import helpers
import dataset as ds
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Global variables
year_freq_df = None
month_year_freq_df = None
generic_specific_freq_df = None
freq_matrix = None
raw_keyword_data = helpers.load_dataset(ds.output_data + "keywords/keywords_tagged.csv")
generic_list = raw_keyword_data[raw_keyword_data.tag == "generic"].term.tolist()
specific_list = raw_keyword_data[raw_keyword_data.tag == "specific"].term.tolist()
years = ["2014", "2015", "2016", "2017", "2018"]

# create_storage_dataframes()
# parameters:
#   None
# returns:
#   None
# description:
#   This function creates the dataframes for storing the frequency based stats
#       by calling the relevant function for each one (this function completes
#       the common parts between the three). Dataframes are stored in global
#       variables.
def create_storage_dataframes():
    global year_freq_df, month_year_freq_df, generic_specific_freq_df, raw_list_data
    cols = ["keyword"]
    year_freq_df = pd.DataFrame(columns = cols)
    month_year_freq_df = pd.DataFrame(columns = cols)
    generic_specific_freq_df = pd.DataFrame(columns = cols)
    keyword_list = raw_keyword_data.term.tolist()
    for keyword in keyword_list:
        year_freq_df = year_freq_df.append({"keyword": keyword}, ignore_index=True)
        month_year_freq_df = month_year_freq_df.append({"keyword": keyword}, ignore_index=True)
        generic_specific_freq_df = generic_specific_freq_df.append({"keyword": keyword}, ignore_index=True)
    year_freq_df = create_year_dataframe(year_freq_df)
    month_year_freq_df = create_month_year_dataframe(month_year_freq_df)
    generic_specific_freq_df = create_generic_specific_dataframe(generic_specific_freq_df)
    return

# create_freq_matrix()
# parameters:
#   None
# returns:
#   None
# description:
#   This function creates a datafram for storing the frequency matrix (how often
#       each keyword is matched with another keyword). It creates the rows and
#       columns required for the matrix which is then stored in a global variable.
def create_freq_matrix():
    global freq_matrix, raw_list_data
    cols = ['keyword']
    freq_matrix = pd.DataFrame(columns = cols)
    keyword_list = raw_keyword_data.term.tolist()
    for keyword in keyword_list:
        freq_matrix = freq_matrix.append({"keyword": keyword}, ignore_index=True)
    for keyword in keyword_list:
        freq_matrix[keyword] = 0
    return

# create_year_dataframe()
# parameters:
#   year_freq_df : DataFrame - the created dataframe so far
# returns:
#   year_freq_df : DataFrame - the dataframe with the year columns added
# description:
#   This function loops over the list of years, and adds them to the dataframe
#       as columns, setting their initial values to zero (for the year dataframe).
def create_year_dataframe(year_freq_df):
    global years
    for year in years:
        year_freq_df[year] = 0
    return year_freq_df

# create_month_year_dataframe()
# parameters:
#   month_year_freq_df : DataFrame - the created dataframe so far
# returns:
#   month_year_freq_df : DataFrame - the dataframe with month, year columns added
# description:
#   This function loops over a list of years, and a range (representing months)
#       and appends the two to create a month_year. This is then added to the
#       dataframe as a column, with an initial value of zero.
def create_month_year_dataframe(month_year_freq_df):
    global years
    for year in years:
        for i in range(1,13):
            col = str(i) + "_" + year
            month_year_freq_df[col] = 0
    return month_year_freq_df

# create_generic_specific_dataframe()
# parameters:
#   generic_specific_freq_df : DataFrame - the created dataframe so far
# returns:
#   generic_specific_freq_df : DataFrame - the dataframe with new columns
# description:
#   This function adds new columns to the dataframe representing the different
#       categories of matches.
def create_generic_specific_dataframe(generic_specific_freq_df):
    generic_specific_freq_df["generic"] = 0
    generic_specific_freq_df["specific"] = 0
    generic_specific_freq_df["generic_specific"] = 0
    generic_specific_freq_df["total"] = 0
    return generic_specific_freq_df

# year_freq_update()
# parameters:
#   keyword : string - the keyword for which a value should be updated
#   year : integer - the year for which the keyword value should be updated
# returns:
#   None
# description:
#   This function updates a value in the year frequency dataframe, by keyword
#       and year.
def year_freq_update(keyword, year):
    global year_freq_df
    df = year_freq_df[year_freq_df.keyword == keyword]
    keyword_index = df.index.tolist()
    if len(keyword_index) == 1:
        year_freq_df.at[keyword_index, year] = year_freq_df[year].loc[keyword_index] + 1
    return

# month_year_freq_update()
# parameters:
#   keyword : string - the keyword for which a value should be updated
#   month : integer - the month for which the keyword value should be updated
#   year : integer - the year for which the keyword value should be updated
# returns:
#   None
# description:
#   This function updates a value in the month, year frequency dataframe,
#       by keyword and year.
def month_year_freq_update(keyword, month, year):
    global month_year_freq_df
    df = month_year_freq_df[month_year_freq_df.keyword == keyword]
    keyword_index = df.index.tolist()
    month_year = str(month) + "_" + str(year)
    if len(keyword_index) == 1:
        month_year_freq_df.at[keyword_index, month_year] = month_year_freq_df[month_year].loc[keyword_index] + 1
    return

# generic_specific_freq_update()
# parameters:
#   keyword : string - the keyword for which a value should be updated
#   category : string - the category for which the keyword value should be updated
# returns:
#   None
# description:
#   This function updates a value in the generic/specific frequency dataframe,
#       by category. This function also modifies the total to reflect the change.
def generic_specific_freq_update(keyword, category):
    global generic_specific_freq_df
    df = generic_specific_freq_df[generic_specific_freq_df.keyword == keyword]
    keyword_index = df.index.tolist()
    if len(keyword_index) == 1:
        generic_specific_freq_df.at[keyword_index, category] = generic_specific_freq_df[category].loc[keyword_index] + 1
        generic_specific_freq_df.at[keyword_index, "total"] = generic_specific_freq_df["total"].loc[keyword_index] + 1
    return

# freq_matrix_update()
# parameters:
#   keywords : list of keywords matched
# returns:
#   None
# description:
#   This function updated the frequency matrix of keyword matches. It is
#       completed by looping over each keyword in the list and updating the
#       matrix in both direction for the current keyword and next keyword.
def freq_matrix_update(keywords):
    global freq_matrix
    if len(keywords) == 1:
        return
    for idx, val in enumerate(keywords):
        jdx = idx + 1
        if jdx > (len(keywords) - 1):
            jdx = 0

        df = freq_matrix[freq_matrix.keyword == val]
        keyword_index = df.index.tolist()
        if len(keyword_index) == 1:
            freq_matrix.at[keyword_index, keywords[jdx]] = freq_matrix[keywords[jdx]].loc[keyword_index] + 1

        # inverse
        df = freq_matrix[freq_matrix.keyword == keywords[jdx]]
        keyword_index = df.index.tolist()
        if len(keyword_index) == 1:
            freq_matrix.at[keyword_index, keywords[idx]] = freq_matrix[keywords[idx]].loc[keyword_index] + 1
    return

# keyword_checker()
# parameters:
#   keyword : string - keyword to be looked for
#   list : list - list of words to be checked against
# returns:
#   True
#       [OR]
#   False
# description:
#   This function loops over a list trying to identify if the given keyword
#       is contained in the list.
def keyword_checker(keyword, list):
    for item in list:
        if item == keyword:
            return True
    return False

# processing()
# parameters:
#   None
# returns:
#   created_files : list - list of created files
# description:
#   This function runs this file by calling the other functions in this file.
#       It loops over each of the matched tweets and uses the matches column
#       to determine the type of matches made, when the tweets are from and in
#       turn updates the three dataframes and the frequency matrix with the
#       results of the comparison. The dataframes are stored and the created
#       filenames and paths are returned in a list.
def processing():
    create_storage_dataframes()
    create_freq_matrix()
    global generic_list, specific_list, year_freq_df, month_year_freq_df, generic_specific_freq_df
    created_files = []
    file_path = ds.output_data + "time_filtered_dataset_extraction/"
    generic_tweets = file_path + "generic.csv"
    specific_tweets = file_path + "specific.csv"
    all_tweets_df = pd.concat([helpers.load_dataset(specific_tweets), helpers.load_dataset(generic_tweets)])
    all_tweets_df.reset_index(inplace=True, drop=True)
    limit = len(all_tweets_df) + 1
    counter = 0
    for index, row in all_tweets_df.iterrows():
        if counter == limit:
            break
        counter += 1
        if index % 100 == 0:
            print("      -", str(index), "/", str(len(all_tweets_df)))
        generic_matches = []
        specific_matches = []
        # preprocessing
        row.matches = row.matches.strip("''][").split("', '")
        for match in row.matches:
            generic_check = keyword_checker(match, generic_list)
            generic_matches.append(generic_check)
            specific_check = keyword_checker(match, specific_list)
            specific_matches.append(specific_check)
            if generic_check | specific_check:
                year_freq_update(match, str(row.year))
                month_year_freq_update(match, str(row.month), str(row.year))
        if True in generic_matches:
            if True in specific_matches:
                for match in row.matches:
                    generic_specific_freq_update(match, "generic_specific")
            else:
                for match in row.matches:
                    generic_specific_freq_update(match, "generic")
        else:
            for match in row.matches:
                generic_specific_freq_update(match, "specific")
        freq_matrix_update(row.matches)

    #store dataframe
    storage_path = ds.output_data + "statistics/"
    helpers.path_checker(storage_path)
    # Store year frequency
    helpers.dataframe_to_csv(year_freq_df, storage_path + "year_frequency.csv")
    created_files.append(storage_path + "year_frequency.csv")
    # Store month year frequency
    helpers.dataframe_to_csv(month_year_freq_df, storage_path + "month_year_frequency.csv")
    created_files.append(storage_path + "month_year_frequency.csv")
    # Store generic specific frequency
    helpers.dataframe_to_csv(generic_specific_freq_df, storage_path + "generic_specific_frequency.csv")
    created_files.append(storage_path + "generic_specific_frequency.csv")
    # Store frequency matrix
    helpers.dataframe_to_csv(freq_matrix, storage_path + "frequency_matrix.csv")
    created_files.append(storage_path + "frequency_matrix.csv")
    month_year_freq_output_files = preprocess_month_year_graph(month_year_freq_df)
    year_freq_output_files = preprocess_year_graph(year_freq_df)
    frequency_total_output_files = preprocess_frequency_total_graph(generic_specific_freq_df)
    for file in month_year_freq_output_files:
        created_files.append(file)
    for file in year_freq_output_files:
        created_files.append(file)
    for file in frequency_total_output_files:
        created_files.append(file)
    return created_files

# run()
# parameters:
#   None
# returns:
#   None
# description:
#   This function is called to run the code in this file (by calling the
#       processing() function) and generates console output detailing the
#       purpose of the file, its progress and output files created.
def run():
    print("\33[93m- statistics.py\33[0m")
    print("  - Generating statistics")
    output_files = processing()
    print("    - Output files:")
    for output_file in output_files:
        print("      - " + output_file)
    print("\n\33[92m  - Complete \33[0m \n")

# preprocess_year_graph()
# parameters:
#   df : DataFrame - the year_freq_df for preprocessing
# returns:
#   created_files : list - list of files created
# description:
#   This function creates a list of the columns in the dataframe (excluding the
#       keyword column), and in turn calls the year_graph_generator() function
#       with a dataframe consisting of keyword and a single year to generate
#       graphs for each individual year.
def preprocess_year_graph(df):
    columns = []
    created_files = []
    for col in df.head():
        if col != "keyword":
            columns.append(col)
    for column in columns:
        temp_df = df[['keyword', column]].copy()
        created_files.append(year_graph_generator(temp_df, column, ds.output_data + "statistics/graphs/year_frequency/"))
    return created_files

# preprocess_month_year_graph()
# parameters:
#   df : DataFrame - the month_year_freq_df for preprocessing
# returns:
#   created_files : list - list of files created
# description:
#   This function creates a list of the columns in the dataframe (excluding the
#       keyword column), and in turn calls the month_year_graph_generator() function
#       with a dataframe consisting of keyword and a single month_year to generate
#       graphs for each individual month_year column.
def preprocess_month_year_graph(df):
    columns = []
    created_files = []
    for col in df.head():
        if col != "keyword":
            columns.append(col)
    for column in columns:
        temp_df = df[['keyword', column]].copy()
        created_files.append(month_year_graph_generator(temp_df, column, ds.output_data + "statistics/graphs/month_year_frequency/" + column.split("_")[1] + "/"))
    return created_files

# preprocess_frequency_total_graph()
# parameters:
#   df : DataFrame - the generic/specific dataframe
# returns:
#   created_files : list - list of created files
# description:
#   This function uses the total column and keywords column and sends them to
#       the total_frequency_graph_generator() function to create a graph showing
#       the total matches for each keyword.
def preprocess_frequency_total_graph(df):
    created_files = []
    temp_df = df[['keyword', 'total']].copy()
    output_files = total_frequency_graph_generator(temp_df, ds.output_data + "statistics/graphs/total_frequency/")
    for file in output_files:
        created_files.append(file)
    return created_files

# year_graph_generator()
# parameters:
#   df : DataFrame - the year_freq_df dataframe
#   col_name : string - column name
#   path : string - path to store the graph in
# returns:
#   filename : string - the created filename and path
# description:
#   This function sorts the dataframe into descending order, and creates a
#       second dataframe containing the top 10 keywords (for the year) for
#       ouputting into a bar chart, which is then created and stored.
def year_graph_generator(df, col_name, path):
    col_name = str(col_name)
    year = col_name
    filename = path + year + ".png"
    helpers.path_checker(path)
    plt.style.use('fivethirtyeight')
    df = df.sort_values(col_name, ascending=False)
    df2 = pd.DataFrame(columns = ['keyword', col_name])
    limit = 10
    counter = 0
    for index, row in df.iterrows():
        if row[col_name] != 0:
            df2 = df2.append(df[df.index == index])
            counter += 1
        if counter == limit:
            break
    df2.set_index('keyword',drop=True,inplace=True)
    if len(df2) > 0:
        ax = df2[col_name].plot.bar(figsize=(20,12.75))
        plt.xlabel("Keyword/Term")
        plt.ylabel("Number of Tweets")
        plt.title("10 most frequent keywords/terms for " + year)
        plt.subplots_adjust(bottom=0.3)
        plt.savefig(filename)
        plt.close()
    return filename

# month_year_graph_generator()
# parameters:
#   df : DataFrame - the month_year_freq_df dataframe
#   col_name : string - column name
#   path : string - path to store the graph in
# returns:
#   filename : string - the created filename and path
# description:
#   This function sorts the dataframe into descending order, and creates a
#       second dataframe containing the top 10 keywords (for the month year) for
#       ouputting into a bar chart, which is then created and stored. the month
#       is also mapped to a name.
def month_year_graph_generator(df, col_name, path):
    col_name = str(col_name)
    month_string = col_name.split("_")[0]
    year = col_name.split("_")[1]
    filename = path + col_name + ".png"
    helpers.path_checker(path)
    month_map = [["1", "January"],
                ["2", "February"],
                ["3", "March"],
                ["4", "April"],
                ["5", "May"],
                ["6", "June"],
                ["7", "July"],
                ["8", "August"],
                ["9", "September"],
                ["10", "October"],
                ["11", "November"],
                ["12", "December"]]
    for month in month_map:
        if month[0] == month_string:
            month_name = month[1]
    plt.style.use('fivethirtyeight')
    df = df.sort_values(col_name, ascending=False)
    df2 = pd.DataFrame(columns = ['keyword', col_name])
    limit = 10
    counter = 0
    for index, row in df.iterrows():
        if row[col_name] != 0:
            df2 = df2.append(df[df.index == index])
            counter += 1
        if counter == limit:
            break
    df2.set_index('keyword',drop=True,inplace=True)
    if len(df2) > 0:
        ax = df2[col_name].plot.bar(figsize=(20,12.75))
        plt.xlabel("Keyword/Term")
        plt.ylabel("Number of Tweets")
        plt.title("10 most frequent keywords/terms for " + month_name + " " + year)
        plt.subplots_adjust(bottom=0.35)
        plt.savefig(filename)
        plt.close()
    return filename

# total_frequency_graph_generator()
# parameters:
#   df : DataFrame - the specific/generic dataframe
#   path : string - path to store the graph in
# returns:
#   filename : string - the created filename and path
#   : string - filename for the zero matches CSV file
# description:
#   This function sorts the dataframe into descending order by total, and outputs
#       the data to a bar chart. Also the keywords with zero matches are stored.
def total_frequency_graph_generator(df, path):
    col_name = "total"
    filename = path + "total_freq.png"
    helpers.path_checker(path)
    plt.style.use('fivethirtyeight')
    df = df.sort_values(col_name, ascending=False)
    zero = df[df[col_name] == 0]
    df = df[df[col_name] != 0]
    helpers.dataframe_to_csv(zero, ds.output_data + "statistics/zero_matches.csv")
    df.set_index('keyword',drop=True,inplace=True)
    if len(df) > 0:
        ax = df[col_name].plot.bar(figsize=(20,12.75))
        plt.xlabel("Keyword/Term")
        plt.ylabel("Number of Tweets")
        plt.title("Keyword frequency")
        plt.subplots_adjust(bottom=0.3)
        plt.savefig(filename)
        plt.close()
    return [filename, ds.output_data + "statistics/zero_matches.csv"]
