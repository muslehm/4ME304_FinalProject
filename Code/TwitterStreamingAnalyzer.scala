package com.twitter.spark.streaming

// Only needed for Spark Streaming.
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import org.apache.spark.sql.functions._

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType



import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

// Only needed for utilities for streaming from Twitter.
import org.apache.spark.streaming.twitter._

import twitter4j.Place

import java.sql.Date

import com.google.gson.Gson
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object TwitterStreamingAnalyzer {
  def main(args: Array[String]){ 
    
   val gson = new Gson()

    
   //Variables that contains the user credentials to access Twitter API 
    val accessToken = ""
    val accessTokenSecret = ""
    val consumerKey = ""
    val consumerSecret = "" 
  
    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
   
    
     
     //Create a SparkConf object 
    val sparkConf = new SparkConf().setAppName("TwitterSimpleAnalyzer").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)	
    // Use the config to create a streaming context that creates a new RDD
    // with a batch interval of every 30 minutes.
    val ssc = new StreamingContext(sc, Seconds(1800))
  
     
     
    // Use the streaming context and the TwitterUtils to create the
    // Twitter stream.
   // val filters = Array("apache","spark")
    val streamTweets = TwitterUtils.createStream(ssc, None, Seq("Brexit"))
   
    //case class Tweet(createdAt:String,text:String,source:String,lang:String)
    val sqlContext = SQLContext.getOrCreate(sc);  
    import sqlContext.implicits._ 
    
     //create empty tweets dataframe
    val schema = StructType(
       StructField("followerCount", IntegerType, false) ::
       StructField("friendsCount", IntegerType, false) ::
       StructField("userName", StringType, false) :: Nil)
    
    var dataRDD = sc.emptyRDD[Row]
    
    var trainingDataDF = sqlContext.sparkSession.createDataFrame(dataRDD, schema)
    var testingDataDF = sqlContext.sparkSession.createDataFrame(dataRDD, schema)
    
    var trainingTweetsCollected:Long = 0;
    var numberTrainingTweets:Long = 700; 
    var testingTweetsCollected:Long = 0;
    var numberTestingTweets:Long = 300; 
    var isTrainingMode = true;
    var isPredictionMode = false;
    val K = 5;
    val kmeans = new KMeans().setK(5).setSeed(1L)    
    
    streamTweets.foreachRDD(rdd => {
        if(!rdd.isEmpty()){
         val tweetsRDD = rdd.map(twt=>(twt.getUser().getFollowersCount,twt.getUser().getFriendsCount,twt.getUser().getName))
         println("Number of tweets in batch: " + tweetsRDD.count())
        
         
         if(isTrainingMode){      
             
          
             if(trainingTweetsCollected<numberTrainingTweets){
                   println("--------------------Collect Training Data--------------------------------")
                   //collect data for model training
                   val newData = tweetsRDD.toDF("followerCount","friendsCount","userName")
                   trainingDataDF = trainingDataDF.union(newData)
                   println("Number of collected training data " + trainingDataDF.count())
                   trainingTweetsCollected+=trainingDataDF.count()
             }
             else{
               
                  //train model 
                  println("--------------------Traine Model--------------------------------")
                  
                  println("Number of records " + trainingDataDF.count())
                  // Clean Data
                  val trainingDataDFC1 = trainingDataDF.where("friendsCount != 0")
                  val trainingDataDFC2 = trainingDataDFC1.where("followerCount != 0")
                  val trainingDataDFC3 = trainingDataDFC2.where("friendsCount < 10000")
                  val trainingDataDFC4 = trainingDataDFC3.where("friendsCount > 20")
                  val trainingDataDFC = trainingDataDFC4.where("followerCount < 400000")
                   //trasnform data to vectors
                  val assembler = new VectorAssembler().setInputCols(Array("followerCount", "friendsCount" ))
                                 .setOutputCol("features") 
                  
                  val preparedData = assembler.transform(trainingDataDFC)
                  //train model
                  val kmeansModel = kmeans.fit(preparedData.cache())
                  
                  //    Evaluate clustering by computing Within Set Sum of Squared Errors.
                  val pdError = kmeansModel.computeCost(preparedData)
                  println(s"Within Set Sum of Squared Errors = $pdError")
                  
                  // Shows the result.
                    println("Cluster Centers: ")
                    kmeansModel.clusterCenters.foreach(println)
                  //save model to file
                    kmeansModel.save("src/main/resources/output/kmeans_model_t")
                    
                  isTrainingMode=false  
           }
           
           
           
         
         }
         else{
           
           
            if(testingTweetsCollected<numberTestingTweets){
              
              println("--------------------Collect Testing Data--------------------------------")
              //collect data for model testing
              testingDataDF = testingDataDF.union(tweetsRDD.toDF("followerCount","friendsCount","userName"))
              println("Number of collected testing data " + testingDataDF.count())
              testingTweetsCollected+=testingDataDF.count()
            }
           
           else{
                //prediction mode
                println("--------------------Prediction Mode--------------------------------")
                
                val testingDataDFC1 = testingDataDF.where("friendsCount != 0")
                  val testingDataDFC2 = testingDataDFC1.where("followerCount != 0")
                  val testingDataDFC3 = testingDataDFC2.where("friendsCount < 10000")
                  val testingDataDFC4 = testingDataDFC3.where("friendsCount > 20")
                  val testingDataDFC = testingDataDFC4.where("followerCount < 400000")
                //transform data to vector  
                  val assembler = new VectorAssembler().setInputCols(Array("followerCount", "friendsCount" ))
                                 .setOutputCol("features") 
                  val preparedData = assembler.transform(testingDataDFC)
                 
                 //load model
                 val model = KMeansModel.load("src/main/resources/output/kmeans_model_t")
                 val kmeans_predictions = model.transform(preparedData.cache())
                 kmeans_predictions.select("prediction").groupBy("prediction").count().sort($"count".desc).show()
                 //show predictions
                 //kmeans_predictions.show()
                 
                 val WSSSET = model.computeCost(preparedData)
                  println(s"Within Set Sum of Squared Errors = $WSSSET")
                  
                  //Save File to use for visualization in R
                  val saveData = kmeans_predictions.select(col("followerCount"), col("friendsCount"), col("userName"))
                  saveData.coalesce(1).write.format("json").save("src/main/resources/output/kmeans_predict.json")
                 
           }
         }
         
       
        }
                        
                
    })
   

   
   
    ssc.start()
    ssc.awaitTermination()
  }
  
  
}