library(ggplot2)
library("rjson")
library(jsonlite)
library(dplyr) 
library(ggplot2)
library(mclust)


#filter(json_data, followers_count > 500000)
#filter(json_data, friends_count > 50000)

json_data <- stream_in(file("~/Downloads/features_tweets.json"))
json_data_as_list <- 0
set.seed(20)
tweetsClusters <- kmeans(json_data[, 1:2], 5, nstart = 20)
table(tweetsClusters$cluster, json_data$location)
ggplot(json_data, aes(followers_count, friends_count, color = tweetsClusters$cluster)) + geom_point()
head(tweetsClusters)

proData<-json_data[!(json_data$followers_count>200000),]
proData<-proData[!(proData$friends_count>50000),]

set.seed(20)
tweetsCluster <- kmeans(proData[, 1:2], 5, nstart = 20)
table(tweetsCluster$cluster, proData$location)
ggplot(proData, aes(followers_count, friends_count, color = tweetsCluster$cluster)) + geom_point()

popData<-json_data[!(json_data$followers_count>50000),]
popData<-popData[!(popData$friends_count>10000),]

set.seed(20)
tweetsCluster <- kmeans(popData[, 1:2], 5, nstart = 20)
table(tweetsCluster$cluster, popData$location)
ggplot(popData, aes(followers_count, friends_count, color = tweetsCluster$cluster)) + geom_point()

json_data$ratio <- with(json_data, friends_count/followers_count)
filter(json_data, ratio > 0.0)

fit <- Mclust(popData[, 1:2])
plot(fit) # plot results 
summary(fit) # display the best model

d <- dist(popData[, 1:2], method = "euclidean") # distance matrix
fit <- hclust(d, method="ward.D") 
plot(fit) # display dendogram
groups <- cutree(fit, k=5) # cut tree into 5 clusters
# draw dendogram with red borders around the 5 clusters 
rect.hclust(fit, k=5, border="red")



###Below code was taken from http://www.sthda.com/english/wiki/text-mining-and-word-cloud-fundamentals-in-r-5-simple-steps-you-should-know
#WordCloud Mapping
json_data <- stream_in(file("~/Downloads/wordcloud.json"))
json_data <- as.list(json_data$filtered)
library(wordcloud) # this requires the tm and NLP packages
library("tm")
docs <- Corpus(VectorSource(json_data))
inspect(docs)

toSpace <- content_transformer(function (x , pattern ) gsub(pattern, " ", x))
docs <- tm_map(docs, toSpace, "/")
docs <- tm_map(docs, toSpace, "@")
docs <- tm_map(docs, toSpace, "\\|")

# Convert the text to lower case
docs <- tm_map(docs, content_transformer(tolower))
# Remove numbers
docs <- tm_map(docs, removeNumbers)
# Remove english common stopwords
docs <- tm_map(docs, removeWords, stopwords("english"))
# Remove punctuations
docs <- tm_map(docs, removePunctuation)
# Eliminate extra white spaces
docs <- tm_map(docs, stripWhitespace)
# specify your stopwords as a character vector
docs <- tm_map(docs, removeWords, c("https", "crt")) 

dtm <- TermDocumentMatrix(docs)
m <- as.matrix(dtm)
v <- sort(rowSums(m),decreasing=TRUE)
d <- data.frame(word = names(v),freq=v)
head(d, 10)
set.seed(1234)
wordcloud(words = d$word, freq = d$freq, min.freq = 1,
          max.words=50, random.order=FALSE, rot.per=0.35, 
          colors=brewer.pal(8, "Dark2"))

