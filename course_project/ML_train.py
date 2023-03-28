#/spark2.4.8/spark-2.4.8-bin-hadoop2.7/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import (OneHotEncoderEstimator, 
                                VectorAssembler, 
                                CountVectorizer, 
                                StringIndexer, 
                                IndexToString)

spark = SparkSession.builder.appName("ML_train").getOrCreate()

traffic_accident = spark.read.parquet('traffic_data/traffic_for_ML')

cat_columns = ['region_id', 'road_category', 'road_type', 'day_of_week']
num_columns = ['latitude', 'longitude', 'pedal_cycles', 'two_wheeled_mv', 'cars_and_taxis',
               'buses_and_coaches', 'lgvs', 'all_hgvs', 'all_mv']
label = 'is_accident'

stages = []
for categoricalCol in cat_columns:
    stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index').setHandleInvalid("keep")
    encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"]).setHandleInvalid("keep")
    stages += [stringIndexer, encoder]

assemblerInputs = [c + "classVec" for c in cat_columns] + num_columns
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features").setHandleInvalid("keep")
stages += [assembler]

lr = LogisticRegression(featuresCol = 'features', labelCol = label, maxIter=10)
stages += [lr]

pipeline = Pipeline(stages=stages)
pipelineModel = pipeline.fit(traffic_accident)
pipelineModel.write().overwrite().save("LR_model")

result_df = pipelineModel.transform(traffic_accident)
result_df.select('count_point_id', 'day_of_week', 'is_accident', 
				 'probability', 'prediction').show(5, truncate=False)