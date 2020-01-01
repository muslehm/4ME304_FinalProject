READ ME

1) The historical data analysis:

Run in Java8 environment

$pyspark

CollectingTweets.ipynb 
----------------
Enter your twitter credentials. 
Run the five boxes in order.
An output file of tweets.json is created with 30000 tweets on Brexit

TwitterDataAnalysis.ipynb
------------------
In the read.json function place the dir of the tweets.json file produced
Run all boxes in order up until Filtering Tweets.
The Filtering Tweets markdown filter the data based on location on four iteration.
Each iteration writes the cleaned data into a folder.
The produced json file should be renamed and place in directory for use in the next itteration

Note: this was done in iterations due to memory limitations by the device.

TweetsAnalysisML
----------------
This project analyzes the cleaned data for user clusters. 
Place the features_tweets.json produced in previous project in your directory
Run all boxes in order up until "Mapping".
If the further mapping functions do not work, an alternative R project is provided

wordCloudAnalysis
-----------------
This project analyzes the cleaned data for word clusters.
Place the features_tweets.json in your directory
Run code box by box up until Extra Featurizing function
Visualize the wordcloud.json in a word cloud with R

tweetanalysis.R
-----------------
a- to visualize user clusters
place features_tweets.json in your directory
run the project line by line up until line 48.

b- to visualize the word cloud
Place wordcloud.json file in directory
run project from lines 52 to 87

c- Lines 89 to 112 contains a code for a basic sentiment analysis
First place the location of your file with the tokenized and cleaned words list, same one used for the word cloud analysis.
The code first prepares the data into a format readable by the method
Import the needed libraries
Run the function that extracts sentiment words and analyzes them.

2) The real-time data analysis:

StreamingKMeans
----------------
Insert your twitter credentials
Run the project box by box and wait for the streaming to finish.

TwitterStreamingAnalyzer.scala
-------------------
Open the project in Scala IDE
Add your twitter credentials
Compile the project by clicking on project folder > Properties > Scala Compiler
Then, run the file as Scala Project and wait for the output
The training model is saved int he output, along with a json file of the dataset used for prediction