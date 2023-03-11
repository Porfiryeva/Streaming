##### Задание 1. Задание как обычно осознать что было на уроке. Повторить со своими данными.


1\.1\. Подключаемся к серверу

```bash
$ ssh raj_ops@127.0.0.1 -p 2222
```

1\.2\. Запускаем `pyspark`. 

- предварительно распаковываем архив с соотв. версией spark в /spark2.4.8
- и указываем в spark-env.sh HADOOP_CONF_DIR
```bash
[raj_ops@sandbox-hdp ~]$ export SPARK_KAFKA_VERSION=0.10
[raj_ops@sandbox-hdp ~]$ /spark2.4.8/spark-2.4.8-bin-hadoop2.7/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --master local[1]
```

1\.3\. Прочитаем топик `lesson3`, вспомним что лежит в Кафке.

Стандартные импорты и константы.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType
kafka_brokers = "sandbox-hdp.hortonworks.com:6667"
```

Создаем стрим, читаем из Кафки.

```python
raw_data = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "lesson3"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "5"). \
    load()
```

- или так - чтение 1 из партиции
```python
raw_data = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "lesson3"). \
    option("startingOffsets", """{"lesson3": {"0": 0, "1": -1, "2": -1}}"""). \
    option("maxOffsetsPerTrigger", "5"). \
    load()
```
Задаём структуру данных, котора содержится в топике.

```python
schema = StructType() \
    .add('Unnamed: 0', IntegerType()) \
    .add('rank', StringType()) \
    .add('trend', StringType()) \
    .add('season', IntegerType()) \
    .add('episode', IntegerType()) \
    .add('name', StringType()) \
    .add('start', IntegerType()) \
    .add('total_votes', StringType()) \
    .add('average_rating', FloatType())
```

Преобразуем данные в соответствии со схемой.
   
```python
parsed_OnePiece = raw_data \
    .select( F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset") \
    .select("value.*", "offset")
parsed_OnePiece = parsed_OnePiece.withColumnRenamed("Unnamed: 0", "Unnamed")
parsed_OnePiece.printSchema()
```
    root
     |-- Unnamed: integer (nullable = true)
     |-- rank: string (nullable = true)
     |-- trend: string (nullable = true)
     |-- season: integer (nullable = true)
     |-- episode: integer (nullable = true)
     |-- name: string (nullable = true)
     |-- start: integer (nullable = true)
     |-- total_votes: string (nullable = true)
     |-- average_rating: float (nullable = true)
     |-- offset: long (nullable = true)


Задаём метод для вывода стрима на консоль.

```python
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=True) \
        .start()
```

Проверяем.

```python
out = console_output(parsed_OnePiece, 5)
out.stop()
```
здесь выводит данные из каждой партиции

    Batch: 0
    -------------------------------------------
    +----------+------+-----+------+-------+--------------------+-----+-----------+--------------+------+
    |Unnamed: 0|  rank|trend|season|episode|                name|start|total_votes|average_rating|offset|
    +----------+------+-----+------+-------+--------------------+-----+-----------+--------------+------+
    |         1|29,290|   11|     1|      2|The Great Swordsm...| 1999|        473|           7.8|     0|
    |         2|32,043|    7|     1|      3|Morgan vs. Luffy!...| 1999|        428|           7.7|     0|
    |         0|24,129|   18|     1|      1|I'm Luffy! The Ma...| 1999|        647|           7.6|     0|
    +----------+------+-----+------+-------+--------------------+-----+-----------+--------------+------+

    23/03/10 08:48:23 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 5000 milliseconds, but spent 6503 milliseconds
    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +----------+------+-----+------+-------+--------------------+-----+-----------+--------------+------+
    |Unnamed: 0|  rank|trend|season|episode|                name|start|total_votes|average_rating|offset|
    +----------+------+-----+------+-------+--------------------+-----+-----------+--------------+------+
    |         4|37,113|    4|     1|      5|Fear, Mysterious ...| 1999|        370|           7.5|     1|
    |         5|36,209|    4|     1|      6|Desperate Situati...| 1999|        364|           7.7|     1|
    |         3|28,818|    8|     1|      4|Luffy's Past! The...| 1999|        449|           8.1|     1|
    +----------+------+-----+------+-------+--------------------+-----+-----------+--------------+------+


```python
out.stop()
```

1\.4\. MEMORY SINK

```python
def memory_sink(df, freq):
    return df.writeStream.format("memory") \
        .queryName("OnePeace") \
        .trigger(processingTime='%s seconds' % freq ) \
        .start()
```

Запускаем стрим.

```python
stream = memory_sink(parsed_OnePiece, 5)
```

Проверим что лежит в таблице `OnePeace`.

```python
spark.sql("select * from OnePeace").show()
```
    Hive Session ID = badd8765-98db-4be2-831f-c1f5d4d6a033
    +----------+------+-----+------+-------+--------------------+-----+-----------+--------------+------+
    |Unnamed: 0|  rank|trend|season|episode|                name|start|total_votes|average_rating|offset|
    +----------+------+-----+------+-------+--------------------+-----+-----------+--------------+------+
    |         1|29,290|   11|     1|      2|The Great Swordsm...| 1999|        473|           7.8|     0|
    |         2|32,043|    7|     1|      3|Morgan vs. Luffy!...| 1999|        428|           7.7|     0|
    |         0|24,129|   18|     1|      1|I'm Luffy! The Ma...| 1999|        647|           7.6|     0|
    |         4|37,113|    4|     1|      5|Fear, Mysterious ...| 1999|        370|           7.5|     1|
    |         5|36,209|    4|     1|      6|Desperate Situati...| 1999|        364|           7.7|     1|
    |         3|28,818|    8|     1|      4|Luffy's Past! The...| 1999|        449|           8.1|     1|
    |         7|38,371|    6|     1|      8|Shousha wa docchi...| 1999|        335|           7.7|     2|
    |         8|42,249|    5|     1|      9|Seigi no usotsuki...| 2000|        327|           7.3|     2|
    |         6|37,648|    4|     1|      7|Sozetsu Ketto! Ke...| 1999|        344|           7.7|     2|
    +----------+------+-----+------+-------+--------------------+-----+-----------+--------------+------+


Посмотрим сколько записей в табличке.

```python
spark.sql("select count(*) from OnePeace").show()
```

    +--------+
    |count(1)|
    +--------+
    |      15|
    +--------+


```python
stream.stop()
```

1\.5\. FILE SINK (only with checkpoint)

```python
def file_sink(df, freq):
    return df.writeStream.format("parquet") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("path","my_parquet_sink") \
        .option("checkpointLocation", "OnePiece_checkpoint") \
        .start()
```

Запускаем стрим на некоторое время.

```python
stream = file_sink(parsed_OnePiece, 5)
stream.stop()
```

Проверим как создались директории чекпойнтов и файлов синка.

```bash
[raj_ops@sandbox-hdp ~]$ hdfs dfs -ls
Found 5 items
drwx------   - raj_ops hdfs          0 2023-03-10 09:09 .Trash
drwxr-xr-x   - raj_ops hdfs          0 2023-03-09 09:38 .sparkStaging
drwxr-xr-x   - raj_ops hdfs          0 2023-03-10 11:07 OnePiece_checkpoint
drwxr-xr-x   - raj_ops hdfs          0 2023-03-07 10:12 input_csv_for_stream
drwxr-xr-x   - raj_ops hdfs          0 2023-03-10 11:14 my_parquet_sink
shadrin_iris_file_checkpoint
```

директория my_parquet_sink:

```bash
[raj_ops@sandbox-hdp ~]$ hdfs dfs -ls my_parquet_sink
Found 4 items
drwxr-xr-x   - raj_ops hdfs          0 2023-03-10 11:14 my_parquet_sink/_spark_metadata
-rw-r--r--   1 raj_ops hdfs       2811 2023-03-10 11:14 my_parquet_sink/part-00000-c0dd0507-e5b5-4080-bb6b-8a6d6304ea00-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2857 2023-03-10 11:14 my_parquet_sink/part-00001-edf49be8-8eac-41ce-99f5-ffb3246df739-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2791 2023-03-10 11:14 my_parquet_sink/part-00002-522f826d-4932-4248-8883-4314d5e330ac-c000.snappy.parquet

```

- чтение из файла
```python
spark.read.parquet("my_parquet_sink").show()
```

    +-------+------+-----+------+-------+--------------------+-----+-----------+--------------+------+
    |Unnamed|  rank|trend|season|episode|                name|start|total_votes|average_rating|offset|
    +-------+------+-----+------+-------+--------------------+-----+-----------+--------------+------+
    |      1|29,290|   11|     1|      2|The Great Swordsm...| 1999|        473|           7.8|     0|
    |     11|40,540|    3|     1|     12|Gekitotsu! Kurone...| 2000|        303|           7.8|     3|
    |      0|24,129|   18|     1|      1|I'm Luffy! The Ma...| 1999|        647|           7.6|     0|
    |      7|38,371|    6|     1|      8|Shousha wa docchi...| 1999|        335|           7.7|     2|
    |      9|41,829|    4|     1|     10|Chijou saikyou no...| 2000|        314|           7.5|     3|
    |      4|37,113|    4|     1|      5|Fear, Mysterious ...| 1999|        370|           7.5|     1|
    |      2|32,043|    7|     1|      3|Morgan vs. Luffy!...| 1999|        428|           7.7|     0|
    |      5|36,209|    4|     1|      6|Desperate Situati...| 1999|        364|           7.7|     1|
    |      6|37,648|    4|     1|      7|Sozetsu Ketto! Ke...| 1999|        344|           7.7|     2|
    |     13|42,445|    2|     1|     14|Rufi fukkatsu! Ka...| 2000|        292|           7.7|     4|
    |     16|39,004|    2|     1|     17|Ikari bakuhatsu! ...| 2000|        296|           8.1|     5|
    |     15|43,088|    2|     1|     16|Kaya o mamore! Us...| 2000|        286|           7.7|     5|
    |     10|43,039|    4|     1|     11|Inbou o abake! Ka...| 2000|        310|           7.4|     3|
    |     12|41,379|    2|     1|     13|Kyoufu no futarig...| 2000|        295|           7.8|     4|
    |      3|28,818|    8|     1|      4|Luffy's Past! The...| 1999|        449|           8.1|     1|
    |     14|44,849|    2|     1|     15|Kuro o taose! Oto...| 2000|        286|           7.5|     4|
    |     20|46,335|    2|     1|     21|Manukarezaru kyak...| 2000|        267|           7.6|     6|
    |     18|34,100|    2|     1|     19|Santouryuu no kak...| 2000|        324|           8.5|     6|
    |     17|49,802|    2|     1|     18|Anta ga chunjuu! ...| 2000|        278|           7.1|     5|
    |     19|43,896|    3|     1|     20|Meibutsu kokku! K...| 2000|        279|           7.7|     6|
    +-------+------+-----+------+-------+--------------------+-----+-----------+--------------+------+
    only showing top 20 rows

```python
spark.read.parquet("my_parquet_sink").select("Unnamed").orderBy("Unnamed").show()
```
    +-------+
    |Unnamed|
    +-------+
    |      0|
    |      1|
    |      2|
    |      3|
    |      4|
    |      5|
    |      6|
    |      7|
    |      8|
    |      9|
    |     10|
    |     11|
    |     12|
    |     13|
    |     14|
    |     15|
    |     16|
    |     17|
    |     18|
    |     19|
    +-------+

1\.6\. KAFKA SINK

```bash
[raj_ops@sandbox-hdp ~]$ /usr/hdp/3.0.1.0-187/kafka/bin/kafka-topics.sh --create --topic OnePiece_sink --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --config retention.ms=-1
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic "OnePiece_sink".
```

Создаём консьюмер, который вычитыват сообщения из топика и пишет их в консоль.

```bash
[raj_ops@sandbox-hdp ~]$ /usr/hdp/3.0.1.0-187/kafka/bin/kafka-console-consumer.sh --topic OnePiece_sink --bootstrap-server sandbox-hdp.hortonworks.com:6667 --from-beginning
```

В первом терминале pyspark определяем метод для записи в созданный топик.
```python
def kafka_sink(df, freq):
    return df.selectExpr("CAST(null AS STRING) as key", "CAST(struct(*) AS STRING) as value") \
        .writeStream \
        .format("kafka") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("topic", "OnePiece_sink") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("checkpointLocation", "OnePiece_kafka_checkpoint") \
        .start()
```

На некоторое время запускаем стрим.

```python
stream = kafka_sink(parsed_OnePiece, 5)
stream.stop()
```

Во втором терминале наблюдаем что в топик `OnePiece_sink` пишутся данные:


    [2, 32,043, 7, 1, 3, Morgan vs. Luffy! Who's This Beautiful Young Girl?, 1999, 428, 7.7, 0]
    [5, 36,209, 4, 1, 6, Desperate Situation! Beast Tamer Mohji vs. Luffy!, 1999, 364, 7.7, 1]
    [8, 42,249, 5, 1, 9, Seigi no usotsuki? Kyaputen Usoppu, 2000, 327, 7.3, 2]
    [11, 40,540, 3, 1, 12, Gekitotsu! Kuroneko kaizokudan sakamichi no daikoubou!, 2000, 303, 7.8, 3]
    [14, 44,849, 2, 1, 15, Kuro o taose! Otoko Usoppu namida no ketsui!, 2000, 286, 7.5, 4]
    [17, 49,802, 2, 1, 18, Anta ga chunjuu! Gaimon to kimyou na nakama, 2000, 278, 7.1, 5]
    [20, 46,335, 2, 1, 21, Manukarezaru kyaku! Sanji no meshi to Gin on, 2000, 267, 7.6, 6]
    [23, 31,727, 4, 1, 24, Tama no me no Mihôku! Kenzoku Zoro umi ni chiru, 2000, 340, 8.7, 7]
    [26, 47,734, 1, 1, 27, Reitetsu hijou no kijin Kaizoku kantai soutaichou Gin, 2000, 257, 7.6, 8]
    [29, 41,958, 1, 1, 30, Tabidachi! Umi no kokku wa Rufi to tomoni, 2000, 268, 8.1, 9]

- пересоздадим топик
```bash
[raj_ops@sandbox-hdp ~]$ /usr/hdp/3.0.1.0-187/kafka/bin/kafka-topics.sh --delete --topic OnePiece_sink --zookeeper localhost:2181 Topic OnePiece_sink is marked for deletion.
[raj_ops@sandbox-hdp ~]$ /usr/hdp/3.0.1.0-187/kafka/bin/kafka-topics.sh --create --topic OnePiece_sink --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --config retention.ms=-1
[raj_ops@sandbox-hdp ~]$ hdfs dfs -rm -R .skipTrash OnePiece_kafka_checkpoint
```
- выведём json
```python
def kafka_sink_json(df, freq):
    return df.selectExpr("CAST(null AS STRING) as key", "CAST(to_json(struct(*)) AS STRING) as value") \
        .writeStream \
        .format("kafka") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("topic", "OnePiece_sink") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("checkpointLocation", "OnePiece_kafka_checkpoint") \
        .start()
```

Запустим на время стрим. 
 
 ```python
stream = kafka_sink_json(parsed_OnePiece, 5)
stream.stop()
```

В соседнем терминале наблюдаем пришедшие данные.

    {"Unnamed":2,"rank":"32,043","trend":"7","season":1,"episode":3,"name":"Morgan vs. Luffy! Who's This Beautiful Young Girl?","start":1999,"total_votes":"428","average_rating":7.7,"offset":0}
    {"Unnamed":5,"rank":"36,209","trend":"4","season":1,"episode":6,"name":"Desperate Situation! Beast Tamer Mohji vs. Luffy!","start":1999,"total_votes":"364","average_rating":7.7,"offset":1}
    {"Unnamed":8,"rank":"42,249","trend":"5","season":1,"episode":9,"name":"Seigi no usotsuki? Kyaputen Usoppu","start":2000,"total_votes":"327","average_rating":7.3,"offset":2}
    {"Unnamed":11,"rank":"40,540","trend":"3","season":1,"episode":12,"name":"Gekitotsu! Kuroneko kaizokudan sakamichi no daikoubou!","start":2000,"total_votes":"303","average_rating":7.8,"offset":3}
    {"Unnamed":14,"rank":"44,849","trend":"2","season":1,"episode":15,"name":"Kuro o taose! Otoko Usoppu namida no ketsui!","start":2000,"total_votes":"286","average_rating":7.5,"offset":4}
    {"Unnamed":17,"rank":"49,802","trend":"2","season":1,"episode":18,"name":"Anta ga chunjuu! Gaimon to kimyou na nakama","start":2000,"total_votes":"278","average_rating":7.1,"offset":5}
    {"Unnamed":20,"rank":"46,335","trend":"2","season":1,"episode":21,"name":"Manukarezaru kyaku! Sanji no meshi to Gin on","start":2000,"total_votes":"267","average_rating":7.6,"offset":6}


В конце удалим топик `OnePiece_sink`.

```bash
[raj_ops@sandbox-hdp ~]$ /usr/hdp/3.0.1.0-187/kafka/bin/kafka-topics.sh --delete --topic OnePiece_sink --zookeeper localhost:2181 
[raj_ops@sandbox-hdp ~]$ hdfs dfs -rm -R .skipTrash OnePiece_kafka_checkpoint
```

1\.7\. FOREACH BATCH SINK

Добавим к датасету `parsed_OnePiece` метку времени.
```python
extended_OnePiece = parsed_OnePiece.withColumn("my_current_time", F.current_timestamp())
```

```python
def foreach_batch_sink(df, freq):
    return  df \
        .writeStream \
        .foreachBatch(foreach_batch_function) \
        .trigger(processingTime='%s seconds' % freq ) \
        .start()
```

Функция, которая будет обрабатывать микробатч:

```python
def foreach_batch_function(df, epoch_id):
    print("starting epoch " + str(epoch_id) )
    print("average values for batch:")
    df.groupBy("trend").avg().show()
    print("finishing epoch " + str(epoch_id))
```

Запустим стрим.

```python
stream = foreach_batch_sink(extended_OnePiece, 5)
stream.stop()
```

В консоли наблюдаем преобразованные микробатчи.


    starting epoch 0
    average values for batch:
    +-----+------------+-----------+------------+----------+-------------------+-----------+
    |trend|avg(Unnamed)|avg(season)|avg(episode)|avg(start)|avg(average_rating)|avg(offset)|
    +-----+------------+-----------+------------+----------+-------------------+-----------+
    |    7|         2.0|        1.0|         3.0|    1999.0|  7.699999809265137|        0.0|
    |    3|        11.0|        1.0|        12.0|    2000.0|  7.800000190734863|        3.0|
    |    5|         8.0|        1.0|         9.0|    2000.0|  7.300000190734863|        2.0|
    |    4|         5.0|        1.0|         6.0|    1999.0|  7.699999809265137|        1.0|
    |    2|        14.0|        1.0|        15.0|    2000.0|                7.5|        4.0|
    +-----+------------+-----------+------------+----------+-------------------+-----------+

    finishing epoch 0
    23/03/11 05:55:15 WARN streaming.ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 5000 milliseconds, but spent 6153 milliseconds
    starting epoch 1
    average values for batch:
    +-----+------------+-----------+------------+----------+-------------------+-----------+
    |trend|avg(Unnamed)|avg(season)|avg(episode)|avg(start)|avg(average_rating)|avg(offset)|
    +-----+------------+-----------+------------+----------+-------------------+-----------+
    |    1|        27.5|        1.0|        28.5|    2000.0| 7.8500001430511475|        8.5|
    |    4|        23.0|        1.0|        24.0|    2000.0|  8.699999809265137|        7.0|
    |    2|        18.5|        1.0|        19.5|    2000.0|  7.349999904632568|        5.5|
    +-----+------------+-----------+------------+----------+-------------------+-----------+

    finishing epoch 1


##### Задание 2. Для DE написать стабильную функцию compaction.
 
```python
import subprocess 

def compact_directory(path):
    df_to_compact = spark.read.parquet(path + "/*.parquet")
    tmp_path = path + "_tmp"
    df_to_compact.write.mode("overwrite").parquet(tmp_path)
    df_to_compact = spark.read.parquet(tmp_path + "/*.parquet")
    df_to_compact.write.mode("overwrite").parquet(path)
    subprocess.call(["hdfs", "dfs", "-rm", "-r", tmp_path])
```

Тестируем.

```python
compact_directory("my_parquet_sink")
```

До упаковки:

```bash 
[raj_ops@sandbox-hdp ~]$ hdfs dfs -ls my_parquet_sink
Found 22 items
drwxr-xr-x   - raj_ops hdfs          0 2023-03-10 11:25 my_parquet_sink/_spark_metadata
-rw-r--r--   1 raj_ops hdfs       2746 2023-03-10 11:25 my_parquet_sink/part-00000-5596f9e4-01f4-4920-90ec-69d7e8ec9f3b-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2773 2023-03-10 11:25 my_parquet_sink/part-00000-5b1fac70-59dd-4a52-be3f-3ac343ddacdc-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2811 2023-03-10 11:24 my_parquet_sink/part-00000-95c22a3b-ff7a-4635-9eb1-05fa81cb0253-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2802 2023-03-10 11:24 my_parquet_sink/part-00000-b3753fe2-f66b-499f-8a87-9e772901f672-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2710 2023-03-10 11:25 my_parquet_sink/part-00000-b4a8fa6f-d033-4871-8c91-843b8d5ce76b-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2811 2023-03-10 11:14 my_parquet_sink/part-00000-c0dd0507-e5b5-4080-bb6b-8a6d6304ea00-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2773 2023-03-10 11:25 my_parquet_sink/part-00000-ff5c5a1d-83be-4ea6-a964-536cafbe8b06-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2737 2023-03-10 11:25 my_parquet_sink/part-00001-16cc3a16-4dc2-4d59-8a61-56814b6dc282-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2830 2023-03-10 11:25 my_parquet_sink/part-00001-57ed84c1-3510-43c4-894d-f06b96ce8773-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2782 2023-03-10 11:24 my_parquet_sink/part-00001-8058f49f-0524-48b9-a337-b8327a674043-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2728 2023-03-10 11:25 my_parquet_sink/part-00001-85655826-62eb-4d9b-84ca-11b8ba4300f1-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2737 2023-03-10 11:25 my_parquet_sink/part-00001-a1b35c66-0b31-4a11-bce6-b4f870d8893d-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2647 2023-03-10 11:24 my_parquet_sink/part-00001-cc2a8834-847b-45b2-a089-a0586a5dd785-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2857 2023-03-10 11:14 my_parquet_sink/part-00001-edf49be8-8eac-41ce-99f5-ffb3246df739-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2737 2023-03-10 11:24 my_parquet_sink/part-00002-1932cdf6-0f98-43e9-8354-f0640901cdfe-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2791 2023-03-10 11:14 my_parquet_sink/part-00002-522f826d-4932-4248-8883-4314d5e330ac-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2773 2023-03-10 11:24 my_parquet_sink/part-00002-7987a53c-28ba-4a6d-abba-082076b65259-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2746 2023-03-10 11:25 my_parquet_sink/part-00002-7a4ea97c-f436-4a13-8348-6b7ab7407957-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2811 2023-03-10 11:25 my_parquet_sink/part-00002-92b31e8d-34d8-4c76-96fc-69214e4154dc-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2737 2023-03-10 11:25 my_parquet_sink/part-00002-c2ad3d76-d2d0-4c89-9148-aa067688e152-c000.snappy.parquet
-rw-r--r--   1 raj_ops hdfs       2755 2023-03-10 11:25 my_parquet_sink/part-00002-f71d3f1f-fb96-4d1d-8001-80e39e1db54e-c000.snappy.parquet
```

После упаковки:

```bash
[raj_ops@sandbox-hdp ~]$ hdfs dfs -ls my_parquet_sink
Found 2 items
-rw-r--r--   1 raj_ops hdfs          0 2023-03-11 06:08 my_parquet_sink/_SUCCESS
-rw-r--r--   1 raj_ops hdfs       4126 2023-03-11 06:08 my_parquet_sink/part-00000-11bae886-d32a-4454-901f-719e19f0cce5-c000.snappy.parquet
```
