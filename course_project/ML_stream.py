#export SPARK_KAFKA_VERSION=0.10
#/spark2.4.8/spark-2.4.8-bin-hadoop2.7/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]

from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, FloatType
from pyspark.sql import functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler, CountVectorizer, StringIndexer, IndexToString

spark = SparkSession.builder.appName("lesson8").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

kafka_brokers = "sandbox-hdp.hortonworks.com:6667"

#read 4 record at a time
traffic = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "traffic"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "4"). \
    load()

schema = StructType() \
    .add("count_point_id", IntegerType()) \
    .add("day_of_week", IntegerType()) \
    .add("pedal_cycles", IntegerType()) \
    .add("two_wheeled_mv", IntegerType()) \
    .add("cars_and_taxis", FloatType()) \
    .add("buses_and_coaches", FloatType()) \
    .add("lgvs", IntegerType()) \
    .add("all_hgvs", FloatType()) \
    .add("all_mv", FloatType())

value_traffic = traffic.select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset")
flat_traffic = value_traffic.select(F.col("value.*"), "offset")

def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=True) \
        .start()

s = console_output(flat_traffic, 5)
s.stop()

###############
# cassandra: use traffic_ml
#preparing DataFrame for requests to Cassandra with historical data
cassandra_features_raw = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="traffic", keyspace="traffic_ml" ) \
    .load()

# one order for columns
cassandra_features_selected = cassandra_features_raw.selectExpr("count_point_id", "day_of_week", "latitude", "longitude",
                                                                "region_id", "road_category", "road_type", "all_hgvs", "all_mv",
                                                                "buses_and_coaches", "cars_and_taxis", "lgvs", "pedal_cycles", 
                                                                "two_wheeled_mv"
                                                                )

cassandra_features_selected.show(3)

#load ML from HDFS
pipeline_model = PipelineModel.load("LR_model")


##########
# foreachBatch logic
def writer_logic(df, epoch_id):
    df.persist()
    print("---------I've got new batch--------")
    print("This is what I've got from Kafka:")
    df.show()
    features_from_kafka = df.groupBy("count_point_id", "day_of_week").agg(F.lit(-999999.0).alias("latitude"), F.lit(-999999.0).alias("longitude"), \
                             F.lit(0).alias("region_id"), F.lit("").alias("road_category"), F.lit("").alias("road_type"),
                             F.max("all_hgvs").alias("all_hgvs"), F.max("all_mv").alias("all_mv"), F.max("buses_and_coaches").alias("buses_and_coaches"), \
                             F.max("cars_and_taxis").alias("cars_and_taxis"), F.max("lgvs").alias("lgvs"), F.max("pedal_cycles").alias("pedal_cycles"), \
                             F.max("two_wheeled_mv").alias("two_wheeled_mv"))
    print("Here is the sums from Kafka:")
    features_from_kafka.show()
    # for where
    cp_per_dw = features_from_kafka.select("count_point_id", "day_of_week").distinct()
    cp_per_dw_list = cp_per_dw.collect()
    cp_per_dw_val = map(lambda x: (str(x.__getattr__("count_point_id")), str(x.__getattr__("day_of_week"))), cp_per_dw_list)
    where1 = [" (count_point_id = " + x[0] + " and day_of_week = " + x[1]  + ") " for x in cp_per_dw_val]
    where2 = " or ".join(where1)
    print("I'm gonna select this from Cassandra:")
    print(where2)
    features_from_cassandra = cassandra_features_selected.where(where2).na.fill(0)
    features_from_cassandra.persist()
    print("Here is what I've got from Cassandra:")
    features_from_cassandra.show()
    cassandra_kafka_union = features_from_kafka.union(features_from_cassandra)
    cassandra_kafka_aggregation = cassandra_kafka_union.groupBy("count_point_id", "day_of_week").agg(F.max("latitude").alias("latitude"), F.max("longitude").alias("longitude"), \
                             F.max("region_id").alias("region_id"), F.max("road_category").alias("road_category"), F.max("road_type").alias("road_type"),
                             F.max("all_hgvs").alias("all_hgvs"), F.max("all_mv").alias("all_mv"), F.max("buses_and_coaches").alias("buses_and_coaches"), \
                             F.max("cars_and_taxis").alias("cars_and_taxis"), F.max("lgvs").alias("lgvs"), 
                             F.max("pedal_cycles").alias("pedal_cycles"), F.max("two_wheeled_mv").alias("two_wheeled_mv"))
    print("Here is how I aggregated Cassandra and Kafka:")
    cassandra_kafka_aggregation.show()
    # prediction
    predict = pipeline_model.transform(cassandra_kafka_aggregation)
    # extract probability
    l = lambda row: float(row[1])
    l_udf = F.udf(l, returnType='float')
    predict_short = predict.select("count_point_id", "day_of_week", "latitude", "longitude", 
        "region_id", "road_category", "road_type", "all_hgvs", "all_mv", "buses_and_coaches", 
        "cars_and_taxis", "lgvs", "pedal_cycles", "two_wheeled_mv", 
        l_udf(F.col("probability")).alias("p_accident"))
    print("It will be written into Cassandra")
    predict_short.show()
    predict_short.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="traffic", keyspace="traffic_ml") \
        .mode("append") \
        .save()
    features_from_cassandra.unpersist()
    print("I saved the prediction and aggregation in Cassandra. Continue...")
    df.unpersist()

stream = flat_traffic \
    .writeStream \
    .trigger(processingTime='100 seconds') \
    .foreachBatch(writer_logic) \
    .option("checkpointLocation", "checkpoints/traffic")

s = stream.start()

while True:
    s.awaitTermination(9)
# s.stop()