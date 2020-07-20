import keyword_single_list_creator
import keyword_tagger
import single_keyword_matching
import keyword_match_corrector
import dataset_extractor
import dataset_merger
import dataset_time_filtering
import statistics

# Creates the list of individual keywords
keyword_single_list_creator.run()
# Tags the keywords for specific/generic/unknown (user input required, done on
#   actual list not split list)
keyword_tagger.run()
# Checks the tweets to identify single keyword matches
single_keyword_matching.run()
# Checks the single keyword matches to see if any combination forms any actual
#   keyword matches
keyword_match_corrector.run()
# Extracts the tweets and stores them
dataset_extractor.run()
# Merges the extracted matches into single files by generic and specific matches
dataset_merger.run()
# Retrieves tweets between 2014 and 2018
dataset_time_filtering.run()
# Executes some statistics and creates graphs
statistics.run()
