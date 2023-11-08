from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType,LongType
from pyspark.sql.types import TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("YouTubeStreamingAnalytics").getOrCreate()
# Define the Kafka broker and topic
kafka_broker = "localhost:9092"  # Replace with your Kafka broker address
videos_kafka_topic = "youtubeData"  # Replace with your Kafka topic name
def saveToPostgresCategories(df, epoch_id):
    # Define the columns that compose your composite primary key
    date_column = "date"
    id_column = "categoryid"

    # Read the existing data from the database
    existing_data = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/YouTubeStreamingAnalytics") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "Categories") \
        .option("user", "postgres") \
        .option("password", "admin") \
        .load()

    # Identify rows in the batch_df that do not exist in the existing_data DataFrame
    new_data = df.join(existing_data, on=[date_column, id_column], how="leftanti")
    print("new Categ ="+str(new_data.count()))
    # If there is new data, write it to the database
    if new_data.count() > 0:
        new_data.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/YouTubeStreamingAnalytics") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "Categories") \
            .option("user", "postgres") \
            .option("password", "admin") \
            .option("conflict", "ignore") \
            .mode("append") \
            .save()

def saveToPostgresVideos(df, epoch_id):
    # Define the columns that compose your composite primary key
    date_column = "date"
    id_column = "id"

    # Read the existing data from the database
    existing_data = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/YouTubeStreamingAnalytics") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "trendsvideos") \
        .option("user", "postgres") \
        .option("password", "admin") \
        .load()

    # Identify rows in the batch_df that do not exist in the existing_data DataFrame
    new_data = df.join(existing_data, on=[date_column, id_column], how="leftanti")
    print("new Videos ="+str(new_data.count()))
    # If there is new data, write it to the database
    if new_data.count() > 0:
        new_data.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/YouTubeStreamingAnalytics") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "trendsvideos") \
            .option("user", "postgres") \
            .option("password", "admin") \
            .option("conflict", "ignore") \
            .mode("append") \
            .save()

def saveToPostgresChannels(df, epoch_id):
    date_column = "date"
    id_column = "id"

    # Read the existing data from the database
    existing_data = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/YouTubeStreamingAnalytics") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "channelsintrends") \
        .option("user", "postgres") \
        .option("password", "admin") \
        .load()

    # Identify rows in the batch_df that do not exist in the existing_data DataFrame
    new_data = df.join(existing_data, on=[date_column, id_column], how="leftanti")
    print("new Channels ="+str(new_data.count()))

    # If there is new data, write it to the database
    if new_data.count() > 0:
        new_data.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/YouTubeStreamingAnalytics") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "channelsintrends") \
            .option("user", "postgres") \
            .option("password", "admin") \
            .option("conflict", "ignore") \
            .mode("append") \
            .save()
    

# Create a Kafka source for Spark Structured Streaming
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", videos_kafka_topic) \
    .load()
# Parse the JSON data from the 'value' column
kafka_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .selectExpr("from_json(value, 'id STRING,date STRING,categoryid STRING, channelid STRING, viewcount STRING, likecount STRING, dislikecount STRING, commentcount STRING') AS data") \
    .select("data.*")

# Perform some basic processing (e.g., convert numeric columns to integers)
kafka_stream = kafka_stream.withColumn("viewcount", col("viewCount").cast("long"))
kafka_stream = kafka_stream.withColumn("categoryid", col("categoryid").cast("long"))
kafka_stream = kafka_stream.withColumn("likecount", col("likeCount").cast("long"))
kafka_stream = kafka_stream.withColumn("dislikecount", col("dislikeCount").cast("long"))
kafka_stream = kafka_stream.withColumn("commentcount", col("commentCount").cast("long"))
kafka_stream = kafka_stream.withColumn("date", col("date").cast("Date"))

#save data
row=kafka_stream
row.writeStream \
    .outputMode("append") \
    .foreachBatch(saveToPostgresVideos) \
    .start()
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", videos_kafka_topic) \
    .load()    
kafka_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .selectExpr("from_json(value, 'id STRING,date STRING,time STRING,categoryid STRING, channelid STRING, viewcount STRING, likecount STRING, dislikecount STRING, commentcount STRING') AS data") \
    .select("data.*")

# Perform some basic processing (e.g., convert numeric columns to integers)
kafka_stream = kafka_stream.withColumn("viewcount", col("viewCount").cast("long"))
kafka_stream = kafka_stream.withColumn("categoryid", col("categoryid").cast("long"))
kafka_stream = kafka_stream.withColumn("likecount", col("likeCount").cast("long"))
kafka_stream = kafka_stream.withColumn("dislikecount", col("dislikeCount").cast("long"))
kafka_stream = kafka_stream.withColumn("commentcount", col("commentCount").cast("long"))
kafka_stream = kafka_stream.withColumn("date", col("date").cast("Date"))
kafka_stream = kafka_stream.withColumn("time", col("time").cast(TimestampType()))

# Calculate some statistics (you can add more)
statistics = kafka_stream.withWatermark("time", "1 minutes") \
.groupBy("categoryid","date").agg(
    expr("count(*) as nbvideo"),
    expr("sum(viewcount) as totalviewcount"),
    expr("sum(likecount) as totallikecount"),
    expr("sum(dislikecount) as totaldislikecount"),
    expr("sum(commentcount) as totalcommentcount"),

)
statistics.writeStream  \
    .outputMode("complete") \
    .foreachBatch(saveToPostgresCategories) \
    .start()
#mostLiked=statistics.sort("totalLikeCount", ascending=False).limit(1)
#mostCommented=statistics.sort("totalCommentCount", ascending=False).limit(1)



# Start the streaming query
#query = statistics.writeStream \
 #   .outputMode("complete") \
  #  .format("console") \
   # .start()



# Define the Kafka topic for channel data
channel_kafka_topic = "channelData"  

# Create a Kafka source for channel data
channel_kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", channel_kafka_topic) \
    .load()
# Parse the JSON data from the 'value' column
channel_kafka_stream = channel_kafka_stream.selectExpr("CAST(value AS STRING)") \
    .selectExpr("from_json(value, 'id STRING, date STRING, title STRING,  publishedat STRING, viewcount STRING, videocount STRING, subscribercount STRING, commentcount STRING') AS data") \
    .select("data.*")

# Perform some basic processing (e.g., convert numeric columns to integers)
channel_kafka_stream = channel_kafka_stream.withColumn("viewcount", col("viewcount").cast("long"))
channel_kafka_stream = channel_kafka_stream.withColumn("videocount", col("videocount").cast("long"))
channel_kafka_stream = channel_kafka_stream.withColumn("subscribercount", col("subscribercount").cast("long"))
channel_kafka_stream = channel_kafka_stream.withColumn("commentcount", col("commentcount").cast("long"))
channel_kafka_stream = channel_kafka_stream.withColumn("date", col("date").cast("Date"))
channel_kafka_stream = channel_kafka_stream.withColumn("publishedat", col("publishedat").cast("Date"))

channel_kafka_stream = channel_kafka_stream.dropDuplicates(['id'])

#ChannelRow=channel_kafka_stream
#ChannelRow=ChannelRow.writeStream \
 #   .outputMode("append") \
  #  .foreachBatch(saveToPostgresChannels) \
   # .start()
#channel_kafka_stream=channel_kafka_stream.groupBy("id").agg(
    #expr("count(*) as channelCount"))
# Start the streaming query for channel statistics
channel_query = channel_kafka_stream.writeStream \
    .outputMode("append")\
    .foreachBatch(saveToPostgresChannels) \
    .start()

# Wait for the channel data query to terminate (e.g., manually stop it or let it run indefinitely)
channel_query.awaitTermination()
# Wait for the query to terminate 
#query.awaitTermination()
#ChannelRow.awaitTermination()