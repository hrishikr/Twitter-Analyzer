# Twitter Analyzer

This project is a tool using Hadoop to process sets of Twitter posts (i.e. tweets) and determining which people, tweets, hashtags, and pairs of hashtags are most popular. The processing of the data will be divided into two passes. 

1. Count the number of times a user, tweet, hashtag or pair of hashtags is mentioned/used across all tweets. This is done in TweetPopularityMR.
2. Sort the results from most to least popular. This is done in TweetSortMR. 

Running the program. You will need to provide the following command-line arguments to the program: <user/tweet/hashtag/hashtag_pair> <cut off> <input file> <output directory>. 
Before running, you must ensure that the output and temp directories do not exist/are deleted, as Hadoop will automatically generate these. 

More information in the spec sheet.
