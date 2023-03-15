## Урок 5. Spark Streaming. Stateful streams.

##### Задание 1. Запустить агрегацию по временному окну в разных режимах.

1\.1\. Подключаемся к серверу

```bash
$ ssh raj_ops@127.0.0.1 -p 2222
```

1\.2\. Запускаем `pyspark`. 
- экспортируем поток Kafka и запускаем спарк2.4
```bash
[raj_ops@sandbox-hdp ~]$ export SPARK_KAFKA_VERSION=0.10
[raj_ops@sandbox-hdp ~]$ /spark2.4.8/spark-2.4.8-bin-hadoop2.7/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --master local[1]
```

1\.3\. Прочитаем топик `OnePiece`, вспомним что лежит в Кафке.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType

kafka_brokers = "sandbox-hdp.hortonworks.com:6667"

raw_data = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "OnePiece"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "6"). \
    load()

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
   
1\.4\. WATERMARK и дубликаты внутри одного батча.

Добавим колонку с временем обработки микробатча. Она понадобится для настройки чекпойнта и вотермарки.
   
```python
extended_OnePiece = raw_data \
    .select( F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset") \
    .select("value.*", "offset").withColumnRenamed("Unnamed: 0", "id") \
    .withColumn("receive_time", F.current_timestamp())

extended_OnePiece.printSchema()
```

    root
     |-- id: integer (nullable = true)
     |-- rank: string (nullable = true)
     |-- trend: string (nullable = true)
     |-- season: integer (nullable = true)
     |-- episode: integer (nullable = true)
     |-- name: string (nullable = true)
     |-- start: integer (nullable = true)
     |-- total_votes: string (nullable = true)
     |-- average_rating: float (nullable = true)
     |-- offset: long (nullable = true)
     |-- receive_time: timestamp (nullable = false)


- для работы с watermarks необходим checkpoint

```python
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("checkpointLocation", "checkpoints/duplicates_console_chk") \
        .options(truncate=True) \
        .start()
```

Запускаем стрим и смотрим как растёт наш чекпойнт (команда `hdfs dfs -du -h checkpoints/duplicates_console_chk`).
```bash
[raj_ops@sandbox-hdp ~]$ hdfs dfs -ls
Found 3 items
drwx------   - raj_ops hdfs          0 2023-03-14 08:43 .Trash
drwxr-xr-x   - raj_ops hdfs          0 2023-03-09 09:38 .sparkStaging
drwxr-xr-x   - raj_ops hdfs          0 2023-03-07 10:12 input_csv_for_stream
```

```python
stream = console_output(extended_OnePiece , 5)
stream.stop()
```

```bash
[raj_ops@sandbox-hdp ~]$ hdfs dfs -du -h checkpoints/
1.4 K  1.4 K  checkpoints/duplicates_console_chk
[raj_ops@sandbox-hdp ~]$ hdfs dfs -du -h checkpoints/
2.3 K  2.3 K  checkpoints/duplicates_console_chk
[raj_ops@sandbox-hdp ~]$ hdfs dfs -du -h checkpoints/
3.2 K  3.2 K  checkpoints/duplicates_console_chk
```

Задаём вотермарку, которая должна очищать чекпоинт. Первый параметр - название колонки, на которую смотрит вотермарка, второй параметр - гарантированное время жизни информации о сообщении в чекпойнте. Именно для этого мы добавляли столбец `receive_time`.

```python
watermarked_OnePiece = extended_OnePiece.withWatermark("receive_time", "30 seconds")
watermarked_OnePiece.printSchema()
```

Схема не поменялась. Вотермарка только следит за чекпойнтом, но никак не аффектит наши данные. 

Проверяем данные на наличие дубликатов

```python
deduplicated_OnePiece = watermarked_OnePiece.drop_duplicates(["average_rating", "receive_time"])
```

Чтобы всё заработало, нужно предварительно очистить чекпойнт (команда `hdfs dfs -rm -r -skipTrash checkpoints/duplicates_console_chk`).


здесь watermark (30 сек.) больше, чем триггер (20 сек.)
```python
stream = console_output(deduplicated_OnePiece , 20)
stream.stop()
```

     -------------------------------------------
    Batch: 0
    -------------------------------------------
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+
    | id|  rank|trend|season|episode|                name|start|total_votes|average_rating|offset|        receive_time|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+
    |  0|24,129|   18|     1|      1|I'm Luffy! The Ma...| 1999|        647|           7.6|     0|2023-03-14 08:56:...|
    |  4|37,113|    4|     1|      5|Fear, Mysterious ...| 1999|        370|           7.5|     4|2023-03-14 08:56:...|
    |  2|32,043|    7|     1|      3|Morgan vs. Luffy!...| 1999|        428|           7.7|     2|2023-03-14 08:56:...|
    |  1|29,290|   11|     1|      2|The Great Swordsm...| 1999|        473|           7.8|     1|2023-03-14 08:56:...|
    |  3|28,818|    8|     1|      4|Luffy's Past! The...| 1999|        449|           8.1|     3|2023-03-14 08:56:...|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+

    23/03/14 08:57:18 WARN streaming.ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 20000 milliseconds, but spent 41556 milliseconds
    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+
    | id|  rank|trend|season|episode|                name|start|total_votes|average_rating|offset|        receive_time|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+
    |  6|37,648|    4|     1|      7|Sozetsu Ketto! Ke...| 1999|        344|           7.7|     6|2023-03-14 08:57:...|
    |  8|42,249|    5|     1|      9|Seigi no usotsuki...| 2000|        327|           7.3|     8|2023-03-14 08:57:...|
    | 11|40,540|    3|     1|     12|Gekitotsu! Kurone...| 2000|        303|           7.8|    11|2023-03-14 08:57:...|
    | 10|43,039|    4|     1|     11|Inbou o abake! Ka...| 2000|        310|           7.4|    10|2023-03-14 08:57:...|
    |  9|41,829|    4|     1|     10|Chijou saikyou no...| 2000|        314|           7.5|     9|2023-03-14 08:57:...|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+



В каждом микробатче только записи с уникальным значением по колонке `average_rating`. По значению в `offset` видно что не все сообщения попадают в вывод.


1\.5\. WINDOW и дубликаты за периоды времени.

Создаём временное окно. В структуру датафрейма добавился новый столбец.
- здесь создаются фиксированные окна
```python
windowed_OnePiece = extended_OnePiece.withColumn("window_time", F.window(F.col("receive_time"), "2 minutes"))
windowed_OnePiece.printSchema()
```

    root
     |-- id: integer (nullable = true)
     |-- rank: string (nullable = true)
     |-- trend: string (nullable = true)
     |-- season: integer (nullable = true)
     |-- episode: integer (nullable = true)
     |-- name: string (nullable = true)
     |-- start: integer (nullable = true)
     |-- total_votes: string (nullable = true)
     |-- average_rating: float (nullable = true)
     |-- offset: long (nullable = true)
     |-- receive_time: timestamp (nullable = false)
     |-- window_time: struct (nullable = false)
     |    |-- start: timestamp (nullable = true)
     |    |-- end: timestamp (nullable = true)


Устанавливаем вотермарку для очистки чекпоинта и удаляем дубли в каждом окне.
Вотермарка устанавливается на столбец с окном:
```python
watermarked_windowed_OnePiece = windowed_OnePiece.withWatermark("window_time", "2 minutes")
deduplicated_windowed_OnePiece = watermarked_windowed_OnePiece \
    .drop_duplicates(["average_rating", "window_time"])
```

очищаем чекпойнт (команда `hdfs dfs -rm -r .skipTrash checkpoints/duplicates_console_chk`)

Проверяем как удаляются дубли из каждого окна.

```python
stream = console_output(deduplicated_windowed_OnePiece , 5)
stream.stop()
```

<details>
<summary>Результат выполнения в консоли</summary>

    -------------------------------------------
    Batch: 0
    -------------------------------------------
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    | id|  rank|trend|season|episode|                name|start|total_votes|average_rating|offset|        receive_time|         window_time|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    |  2|32,043|    7|     1|      3|Morgan vs. Luffy!...| 1999|        428|           7.7|     2|2023-03-14 10:27:...|[2023-03-14 10:26...|
    |  4|37,113|    4|     1|      5|Fear, Mysterious ...| 1999|        370|           7.5|     4|2023-03-14 10:27:...|[2023-03-14 10:26...|
    |  3|28,818|    8|     1|      4|Luffy's Past! The...| 1999|        449|           8.1|     3|2023-03-14 10:27:...|[2023-03-14 10:26...|
    |  1|29,290|   11|     1|      2|The Great Swordsm...| 1999|        473|           7.8|     1|2023-03-14 10:27:...|[2023-03-14 10:26...|
    |  0|24,129|   18|     1|      1|I'm Luffy! The Ma...| 1999|        647|           7.6|     0|2023-03-14 10:27:...|[2023-03-14 10:26...|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+

    23/03/14 10:28:05 WARN streaming.ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 5000 milliseconds, but spent 44472 milliseconds
    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    | id|  rank|trend|season|episode|                name|start|total_votes|average_rating|offset|        receive_time|         window_time|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    | 10|43,039|    4|     1|     11|Inbou o abake! Ka...| 2000|        310|           7.4|    10|2023-03-14 10:28:...|[2023-03-14 10:28...|
    | 11|40,540|    3|     1|     12|Gekitotsu! Kurone...| 2000|        303|           7.8|    11|2023-03-14 10:28:...|[2023-03-14 10:28...|
    |  9|41,829|    4|     1|     10|Chijou saikyou no...| 2000|        314|           7.5|     9|2023-03-14 10:28:...|[2023-03-14 10:28...|
    |  8|42,249|    5|     1|      9|Seigi no usotsuki...| 2000|        327|           7.3|     8|2023-03-14 10:28:...|[2023-03-14 10:28...|
    |  6|37,648|    4|     1|      7|Sozetsu Ketto! Ke...| 1999|        344|           7.7|     6|2023-03-14 10:28:...|[2023-03-14 10:28...|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+


</details>

Видим что в рамках одного окна дубликатов нет.

- но здесь для того, чтобы 1 батч успел обработаться - необходимо около 50 сек на триггер
```python
stream = console_output(deduplicated_windowed_OnePiece , 50)
stream.stop()
```

    -------------------------------------------
    Batch: 0
    -------------------------------------------
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    | id|  rank|trend|season|episode|                name|start|total_votes|average_rating|offset|        receive_time|         window_time|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    |  2|32,043|    7|     1|      3|Morgan vs. Luffy!...| 1999|        428|           7.7|     2|2023-03-14 10:34:...|[2023-03-14 10:34...|
    |  3|28,818|    8|     1|      4|Luffy's Past! The...| 1999|        449|           8.1|     3|2023-03-14 10:34:...|[2023-03-14 10:34...|
    |  4|37,113|    4|     1|      5|Fear, Mysterious ...| 1999|        370|           7.5|     4|2023-03-14 10:34:...|[2023-03-14 10:34...|
    |  0|24,129|   18|     1|      1|I'm Luffy! The Ma...| 1999|        647|           7.6|     0|2023-03-14 10:34:...|[2023-03-14 10:34...|
    |  1|29,290|   11|     1|      2|The Great Swordsm...| 1999|        473|           7.8|     1|2023-03-14 10:34:...|[2023-03-14 10:34...|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+

    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    | id|  rank|trend|season|episode|                name|start|total_votes|average_rating|offset|        receive_time|         window_time|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    | 10|43,039|    4|     1|     11|Inbou o abake! Ka...| 2000|        310|           7.4|    10|2023-03-14 10:35:...|[2023-03-14 10:34...|
    |  8|42,249|    5|     1|      9|Seigi no usotsuki...| 2000|        327|           7.3|     8|2023-03-14 10:35:...|[2023-03-14 10:34...|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+

    -------------------------------------------
    Batch: 2
    -------------------------------------------
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    | id|  rank|trend|season|episode|                name|start|total_votes|average_rating|offset|        receive_time|         window_time|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    | 17|49,802|    2|     1|     18|Anta ga chunjuu! ...| 2000|        278|           7.1|    17|2023-03-14 10:35:...|[2023-03-14 10:34...|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+

    -------------------------------------------
    Batch: 3
    -------------------------------------------
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    | id|  rank|trend|season|episode|                name|start|total_votes|average_rating|offset|        receive_time|         window_time|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    | 18|34,100|    2|     1|     19|Santouryuu no kak...| 2000|        324|           8.5|    18|2023-03-14 10:36:...|[2023-03-14 10:36...|
    | 23|31,727|    4|     1|     24|Tama no me no Mih...| 2000|        340|           8.7|    23|2023-03-14 10:36:...|[2023-03-14 10:36...|
    | 19|43,896|    3|     1|     20|Meibutsu kokku! K...| 2000|        279|           7.7|    19|2023-03-14 10:36:...|[2023-03-14 10:36...|
    | 20|46,335|    2|     1|     21|Manukarezaru kyak...| 2000|        267|           7.6|    20|2023-03-14 10:36:...|[2023-03-14 10:36...|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+



1\.6\. SLIDING WINDOW

Аналогично предыдущему пункту создаём дополнительное поле `sliding_time`.

```python
sliding_OnePiece = extended_OnePiece.withColumn("sliding_time", F.window(F.col("receive_time"), "1 minute", "30 seconds"))
watermarked_sliding_OnePiece = sliding_OnePiece.withWatermark("sliding_time", "2 minutes")
deduplicated_sliding_OnePiece = watermarked_sliding_OnePiece.drop_duplicates(["average_rating", "sliding_time"])
deduplicated_sliding_OnePiece.printSchema()
```

    root
     |-- id: integer (nullable = true)
     |-- rank: string (nullable = true)
     |-- trend: string (nullable = true)
     |-- season: integer (nullable = true)
     |-- episode: integer (nullable = true)
     |-- name: string (nullable = true)
     |-- start: integer (nullable = true)
     |-- total_votes: string (nullable = true)
     |-- average_rating: float (nullable = true)
     |-- offset: long (nullable = true)
     |-- receive_time: timestamp (nullable = false)
     |-- sliding_time: struct (nullable = true)
     |    |-- start: timestamp (nullable = true)
     |    |-- end: timestamp (nullable = true)

- очищаем чекпоинты `hdfs dfs -rm -r -skipTrash checkpoints/duplicates_console_chk`

Запускаем стрим.

```python
stream = console_output(deduplicated_sliding_OnePiece , 20)
stream.stop()
```

<details>
<summary>Результат выполнения в консоли</summary>

    -------------------------------------------
    Batch: 0
    -------------------------------------------
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    | id|  rank|trend|season|episode|                name|start|total_votes|average_rating|offset|        receive_time|        sliding_time|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    |  0|24,129|   18|     1|      1|I'm Luffy! The Ma...| 1999|        647|           7.6|     0|2023-03-14 10:51:...|[2023-03-14 10:51...|
    |  1|29,290|   11|     1|      2|The Great Swordsm...| 1999|        473|           7.8|     1|2023-03-14 10:51:...|[2023-03-14 10:51...|
    |  3|28,818|    8|     1|      4|Luffy's Past! The...| 1999|        449|           8.1|     3|2023-03-14 10:51:...|[2023-03-14 10:51...|
    |  3|28,818|    8|     1|      4|Luffy's Past! The...| 1999|        449|           8.1|     3|2023-03-14 10:51:...|[2023-03-14 10:51...|
    |  2|32,043|    7|     1|      3|Morgan vs. Luffy!...| 1999|        428|           7.7|     2|2023-03-14 10:51:...|[2023-03-14 10:51...|
    |  1|29,290|   11|     1|      2|The Great Swordsm...| 1999|        473|           7.8|     1|2023-03-14 10:51:...|[2023-03-14 10:51...|
    |  4|37,113|    4|     1|      5|Fear, Mysterious ...| 1999|        370|           7.5|     4|2023-03-14 10:51:...|[2023-03-14 10:51...|
    |  4|37,113|    4|     1|      5|Fear, Mysterious ...| 1999|        370|           7.5|     4|2023-03-14 10:51:...|[2023-03-14 10:51...|
    |  0|24,129|   18|     1|      1|I'm Luffy! The Ma...| 1999|        647|           7.6|     0|2023-03-14 10:51:...|[2023-03-14 10:51...|
    |  2|32,043|    7|     1|      3|Morgan vs. Luffy!...| 1999|        428|           7.7|     2|2023-03-14 10:51:...|[2023-03-14 10:51...|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+

    23/03/14 10:52:16 WARN streaming.ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 20000 milliseconds, but spent 34405 milliseconds
    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    | id|  rank|trend|season|episode|                name|start|total_votes|average_rating|offset|        receive_time|        sliding_time|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    | 10|43,039|    4|     1|     11|Inbou o abake! Ka...| 2000|        310|           7.4|    10|2023-03-14 10:52:...|[2023-03-14 10:52...|
    | 10|43,039|    4|     1|     11|Inbou o abake! Ka...| 2000|        310|           7.4|    10|2023-03-14 10:52:...|[2023-03-14 10:51...|
    |  8|42,249|    5|     1|      9|Seigi no usotsuki...| 2000|        327|           7.3|     8|2023-03-14 10:52:...|[2023-03-14 10:51...|
    |  6|37,648|    4|     1|      7|Sozetsu Ketto! Ke...| 1999|        344|           7.7|     6|2023-03-14 10:52:...|[2023-03-14 10:52...|
    |  8|42,249|    5|     1|      9|Seigi no usotsuki...| 2000|        327|           7.3|     8|2023-03-14 10:52:...|[2023-03-14 10:52...|
    | 11|40,540|    3|     1|     12|Gekitotsu! Kurone...| 2000|        303|           7.8|    11|2023-03-14 10:52:...|[2023-03-14 10:52...|
    |  9|41,829|    4|     1|     10|Chijou saikyou no...| 2000|        314|           7.5|     9|2023-03-14 10:52:...|[2023-03-14 10:52...|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+

    23/03/14 10:52:55 WARN streaming.ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 20000 milliseconds, but spent 39367 milliseconds
    -------------------------------------------
    Batch: 2
    -------------------------------------------
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    | id|  rank|trend|season|episode|                name|start|total_votes|average_rating|offset|        receive_time|        sliding_time|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+
    | 16|39,004|    2|     1|     17|Ikari bakuhatsu! ...| 2000|        296|           8.1|    16|2023-03-14 10:52:...|[2023-03-14 10:52...|
    | 17|49,802|    2|     1|     18|Anta ga chunjuu! ...| 2000|        278|           7.1|    17|2023-03-14 10:52:...|[2023-03-14 10:52...|
    | 14|44,849|    2|     1|     15|Kuro o taose! Oto...| 2000|        286|           7.5|    14|2023-03-14 10:52:...|[2023-03-14 10:52...|
    | 17|49,802|    2|     1|     18|Anta ga chunjuu! ...| 2000|        278|           7.1|    17|2023-03-14 10:52:...|[2023-03-14 10:52...|
    | 13|42,445|    2|     1|     14|Rufi fukkatsu! Ka...| 2000|        292|           7.7|    13|2023-03-14 10:52:...|[2023-03-14 10:52...|
    | 12|41,379|    2|     1|     13|Kyoufu no futarig...| 2000|        295|           7.8|    12|2023-03-14 10:52:...|[2023-03-14 10:52...|
    | 16|39,004|    2|     1|     17|Ikari bakuhatsu! ...| 2000|        296|           8.1|    16|2023-03-14 10:52:...|[2023-03-14 10:52...|
    +---+------+-----+------+-------+--------------------+-----+-----------+--------------+------+--------------------+--------------------+

    
</details>

Тут так же видим что в рамках каждого окна нет дубликатов. В 0 миробатче видно, как одна и та же запись с `average_rating = 8.1` является новой для двух окон.


1\.7\. OUTPUT MODES - считаем суммы

Переопределяем метод `console_output` так, чтобы можно было задавать режим вывода результата работы аггрегационных функций.

```python
def console_output(df, freq, out_mode):
    return df.writeStream.format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=False) \
        .option("checkpointLocation", "checkpoints/watermark_console_chk2") \
        .outputMode(out_mode) \
        .start()
```

Используем ранее созданный датафрейм с вотермаркой на 2 минуты. Группируем данные по нескользящему окну `window_time`. 

```python
count_OnePiece = watermarked_windowed_OnePiece.groupBy("window_time").count()
```

Перед каждым запуском очищаем чекпойнт командой `hdfs dfs -rm -r checkpoints/watermark_console_chk2`.

###### update

```python
stream = console_output(count_OnePiece , 50, "update")
stream.stop()
```

<details>
<summary>Результат выполнения в консоли</summary>

    -------------------------------------------
    Batch: 0
    -------------------------------------------
    +------------------------------------------+-----+
    |window_time                               |count|
    +------------------------------------------+-----+
    |[2023-03-14 11:04:00, 2023-03-14 11:06:00]|6    |
    +------------------------------------------+-----+

    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +------------------------------------------+-----+
    |window_time                               |count|
    +------------------------------------------+-----+
    |[2023-03-14 11:06:00, 2023-03-14 11:08:00]|6    |
    +------------------------------------------+-----+

    -------------------------------------------
    Batch: 2
    -------------------------------------------
    +------------------------------------------+-----+
    |window_time                               |count|
    +------------------------------------------+-----+
    |[2023-03-14 11:06:00, 2023-03-14 11:08:00]|12   |
    +------------------------------------------+-----+

    -------------------------------------------
    Batch: 3
    -------------------------------------------
    +------------------------------------------+-----+
    |window_time                               |count|
    +------------------------------------------+-----+
    |[2023-03-14 11:06:00, 2023-03-14 11:08:00]|18   |
    +------------------------------------------+-----+


</details>

В режиме `.outputMode("update")` в консоль пишутся только обновляющиеся записи. Считатся `count` по каждому окну и выводятся записи только о тех окнах, в которых значение поменялось. 


###### complete

```python
stream = console_output(count_OnePiece , 50, "complete")
stream.stop()
```

<details>
<summary>Результат выполнения в консоли</summary>

    -------------------------------------------
    Batch: 0
    -------------------------------------------
    +------------------------------------------+-----+
    |window_time                               |count|
    +------------------------------------------+-----+
    |[2023-03-14 11:14:00, 2023-03-14 11:16:00]|6    |
    +------------------------------------------+-----+

    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +------------------------------------------+-----+
    |window_time                               |count|
    +------------------------------------------+-----+
    |[2023-03-14 11:16:00, 2023-03-14 11:18:00]|6    |
    |[2023-03-14 11:14:00, 2023-03-14 11:16:00]|6    |
    +------------------------------------------+-----+

    -------------------------------------------
    Batch: 2
    -------------------------------------------
    +------------------------------------------+-----+
    |window_time                               |count|
    +------------------------------------------+-----+
    |[2023-03-14 11:16:00, 2023-03-14 11:18:00]|12   |
    |[2023-03-14 11:14:00, 2023-03-14 11:16:00]|6    |
    +------------------------------------------+-----+

    -------------------------------------------
    Batch: 3
    -------------------------------------------
    +------------------------------------------+-----+
    |window_time                               |count|
    +------------------------------------------+-----+
    |[2023-03-14 11:16:00, 2023-03-14 11:18:00]|18   |
    |[2023-03-14 11:14:00, 2023-03-14 11:16:00]|6    |
    +------------------------------------------+-----+

    -------------------------------------------
    Batch: 4
    -------------------------------------------
    +------------------------------------------+-----+
    |window_time                               |count|
    +------------------------------------------+-----+
    |[2023-03-14 11:16:00, 2023-03-14 11:18:00]|18   |
    |[2023-03-14 11:18:00, 2023-03-14 11:20:00]|6    |
    |[2023-03-14 11:14:00, 2023-03-14 11:16:00]|6    |
    +------------------------------------------+-----+


</details>
    

###### append

Пишем все записи только один раз. Информация выводится один раз, когда окно заканчивается.
- режим по умолчанию

```python
stream = console_output(count_OnePiece , 50, "append")
stream.stop()
```

- все батчи пустые, данный режим не поддерживается для данной аггрегирующей функции.


1\.8\. Наблюдаем за суммами в плавающем окне.

```python
sliding_OnePiece = watermarked_sliding_OnePiece.groupBy("sliding_time").count()
stream = console_output(sliding_OnePiece , 20, "update")
stream.stop()
```

<details>
<summary>Результат выполнения в консоли</summary>

    -------------------------------------------
    Batch: 0
    -------------------------------------------
    +------------------------------------------+-----+
    |sliding_time                              |count|
    +------------------------------------------+-----+
    |[2023-03-14 11:27:00, 2023-03-14 11:28:00]|6    |
    |[2023-03-14 11:27:30, 2023-03-14 11:28:30]|6    |
    +------------------------------------------+-----+

    23/03/14 11:28:27 WARN streaming.ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 20000 milliseconds, but spent 42346 milliseconds
    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +------------------------------------------+-----+
    |sliding_time                              |count|
    +------------------------------------------+-----+
    |[2023-03-14 11:28:00, 2023-03-14 11:29:00]|6    |
    |[2023-03-14 11:27:30, 2023-03-14 11:28:30]|12   |
    +------------------------------------------+-----+


</details>

Наблюдаем только обновляющиеся записи в каждом плавающем окне. 

##### Задание 2. Сджойнить стрим со статикой.

Создадим статический датафрейм, который будет расширять исходный датасет.

```python
static_df_schema = StructType() \
    .add("average_rating", FloatType()) \
    .add("description", StringType())

static_df_data = ((7.5, "medium"), (7.6, "medium"), (7.7, "medium"), (7.8, "medium"), \
    (7.9, "medium"), (8.0, "high"), (8.1, "high"), (8.2, "high"), (8.3, "high"), \
    (8.4, "high"), (8.5, "high"), (8.6, "very high"), (8.7, "very high"), (8.8, "very high"))
    
static_df = spark.createDataFrame(static_df_data, static_df_schema)
```

```python
static_joined = watermarked_OnePiece.join(static_df, "average_rating", "left")
static_joined.isStreaming
```

    True
    
После джойна стрима со статикой получаем стрим. 
    
```python
static_joined.printSchema()
```

    root
     |-- average_rating: float (nullable = true)
     |-- id: integer (nullable = true)
     |-- rank: string (nullable = true)
     |-- trend: string (nullable = true)
     |-- season: integer (nullable = true)
     |-- episode: integer (nullable = true)
     |-- name: string (nullable = true)
     |-- start: integer (nullable = true)
     |-- total_votes: string (nullable = true)
     |-- offset: long (nullable = true)
     |-- receive_time: timestamp (nullable = false)
     |-- description: string (nullable = true)


Добавлась колонка `description`.

```python
stream = console_output(static_joined , 20, "update")
stream.stop()
```

    -------------------------------------------                                     
        Batch: 0
    -------------------------------------------
    +--------------+---+------+-----+------+-------+--------------------------------------------------------+-----+-----------+------+-----------------------+-----------+
    |average_rating|id |rank  |trend|season|episode|name                                                    |start|total_votes|offset|receive_time           |description|
    +--------------+---+------+-----+------+-------+--------------------------------------------------------+-----+-----------+------+-----------------------+-----------+
    |7.6           |0  |24,129|18   |1     |1      |I'm Luffy! The Man Who Will Become the Pirate King!     |1999 |647        |0     |2023-03-15 06:46:10.375|medium     |
    |7.7           |2  |32,043|7    |1     |3      |Morgan vs. Luffy! Who's This Beautiful Young Girl?      |1999 |428        |2     |2023-03-15 06:46:10.375|medium     |
    |7.7           |5  |36,209|4    |1     |6      |Desperate Situation! Beast Tamer Mohji vs. Luffy!       |1999 |364        |5     |2023-03-15 06:46:10.375|medium     |
    |7.8           |1  |29,290|11   |1     |2      |The Great Swordsman Appears! Pirate Hunter, Roronoa Zoro|1999 |473        |1     |2023-03-15 06:46:10.375|medium     |
    |8.1           |3  |28,818|8    |1     |4      |Luffy's Past! The Red-haired Shanks Appears!            |1999 |449        |3     |2023-03-15 06:46:10.375|high       |
    |7.5           |4  |37,113|4    |1     |5      |Fear, Mysterious Power! Pirate Clown Captain Buggy!     |1999 |370        |4     |2023-03-15 06:46:10.375|medium     |
    +--------------+---+------+-----+------+-------+--------------------------------------------------------+-----+-----------+------+-----------------------+-----------+

    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +--------------+---+------+-----+------+-------+------------------------------------------------------+-----+-----------+------+-----------------------+-----------+
    |average_rating|id |rank  |trend|season|episode|name                                                  |start|total_votes|offset|receive_time           |description|
    +--------------+---+------+-----+------+-------+------------------------------------------------------+-----+-----------+------+-----------------------+-----------+
    |7.7           |6  |37,648|4    |1     |7      |Sozetsu Ketto! Kengo Zoro VS Kyokugei no Kabaji!      |1999 |344        |6     |2023-03-15 06:46:20.004|medium     |
    |7.7           |7  |38,371|6    |1     |8      |Shousha wa docchi? Akuma no mi no nouryoku taiketsu!  |1999 |335        |7     |2023-03-15 06:46:20.004|medium     |
    |7.8           |11 |40,540|3    |1     |12     |Gekitotsu! Kuroneko kaizokudan sakamichi no daikoubou!|2000 |303        |11    |2023-03-15 06:46:20.004|medium     |
    |7.4           |10 |43,039|4    |1     |11     |Inbou o abake! Kaizoku shitsuji Kyaputen Kuro         |2000 |310        |10    |2023-03-15 06:46:20.004|null       |
    |7.5           |9  |41,829|4    |1     |10     |Chijou saikyou no hen na yatsu! Saiminjutsushi Jango  |2000 |314        |9     |2023-03-15 06:46:20.004|medium     |
    |7.3           |8  |42,249|5    |1     |9      |Seigi no usotsuki? Kyaputen Usoppu                    |2000 |327        |8     |2023-03-15 06:46:20.004|null       |
    +--------------+---+------+-----+------+-------+------------------------------------------------------+-----+-----------+------+-----------------------+-----------+



##### Задание 3. Сджойнить стрим со стримом.

Это задание сделаем на примере датасетов товаров и заказов.
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_order_items_dataset.csv
- предварительно загружаем файлы и создаём два топика `order_items` и `orders`
```bash
$ scp -P 2222 -r '/d/docs/GB/Потоковая обработка данных/lesson5/for_stream/' raj_ops@127.0.0.1:/home/raj_ops/
[raj_ops@sandbox-hdp for_stream] hdfs dfs -mkdir for_stream
[raj_ops@sandbox-hdp for_stream]$ hdfs dfs -put order_items.csv for_stream/order_items/
[raj_ops@sandbox-hdp for_stream]$ hdfs dfs -put orders.csv for_stream/orders/

[raj_ops@sandbox-hdp ~]$ /usr/hdp/3.0.1.0-187/kafka/bin/kafka-topics.sh --create --topic order_items --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --config retention.ms=-1
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic "order_items".
[raj_ops@sandbox-hdp ~]$ /usr/hdp/3.0.1.0-187/kafka/bin/kafka-topics.sh --create --topic orders --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --config retention.ms=-1
Created topic "orders".
```
- загружаем данные из файлов в оба топика (хотя можно было бы стримить из файла)
```python
def kafka_sink(df, freq, topic, check_point):
    return df.selectExpr("CAST(null AS STRING) as key", "CAST(struct(*) AS STRING) as value") \
        .writeStream \
        .format("kafka") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("topic", topic) \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("checkpointLocation", check_point) \
        .start()

schema_orders_items = StructType() \
    .add("order_id", StringType()) \
    .add("order_item_id", StringType()) \
    .add("product_id", StringType()) \
    .add("seller_id", StringType()) \
    .add("shipping_limit_date", StringType()) \
    .add("price", StringType()) \
    .add("freight_value", StringType())

schema_orders = StructType() \
    .add("order_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("order_status", StringType()) \
    .add("order_purchase_timestamp", StringType()) \
    .add("order_approved_at", StringType()) \
    .add("order_delivered_carrier_date", StringType()) \
    .add("order_delivered_customer_date", StringType()) \
    .add("order_estimated_delivery_date", StringType())

order_items = spark \
    .readStream \
    .format("csv") \
    .schema(schema_orders_items) \
    .options(path="for_stream/order_items", header=True) \
    .load()

orders = spark \
    .readStream \
    .format("csv") \
    .schema(schema_orders) \
    .options(path="for_stream/orders", header=True) \
    .load()      
```
На некоторое время запускаем стрим

```python
stream = kafka_sink(order_items, 5, "order_items", "order_items_checkpoint")
stream.stop()

stream = kafka_sink(orders, 5, "orders", "orders_checkpoint")
stream.stop()
```
проверяем в другом окне оба топик
```bash
[raj_ops@sandbox-hdp ~]$ /usr/hdp/3.0.1.0-187/kafka/bin/kafka-console-consumer.sh --topic order_items --bootstrap-server sandbox-hdp.hortonworks.com:6667 --from-beginning
[raj_ops@sandbox-hdp ~]$ /usr/hdp/3.0.1.0-187/kafka/bin/kafka-console-consumer.sh --topic orders --bootstrap-server sandbox-hdp.hortonworks.com:6667 --from-beginning
```


Датасет, соотносящий товары и заказы читаем из кафки, топик `order_items`.

```python
raw_orders_items = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "order_items"). \
    option("startingOffsets", "earliest"). \
    load()
```

Разбираем value и добавляем окно.

```python
extended_orders_items = raw_orders_items \
    .select(F.from_json(F.col("value").cast("String"), schema_orders_items).alias("value")) \
    .select("value.*") \
    .withColumn("order_items_receive_time", F.current_timestamp()) \
    .withColumn("window_time",F.window(F.col("order_items_receive_time"),"2 minutes"))

extended_orders_items.printSchema()
```

    root
     |-- order_id: string (nullable = true)
     |-- order_item_id: string (nullable = true)
     |-- product_id: string (nullable = true)
     |-- seller_id: string (nullable = true)
     |-- shipping_limit_date: string (nullable = true)
     |-- price: string (nullable = true)
     |-- freight_value: string (nullable = true)
     |-- order_items_receive_time: timestamp (nullable = false)
     |-- window_time: struct (nullable = false)
     |    |-- start: timestamp (nullable = true)
     |    |-- end: timestamp (nullable = true)


Датасет списка заказов читаем из кафки, топик `orders`.

```python
raw_orders = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "orders"). \
    option("maxOffsetsPerTrigger", "5"). \
    option("startingOffsets", "earliest"). \
    load()
```

Разбираем value, добавляем колонку со временем получения сообщения,создаём по ней окно и добавляем вотермарку.

```python
watermarked_windowed_orders = raw_orders \
    .select(F.from_json(F.col("value").cast("String"), schema_orders).alias("value"), "offset") \
    .select("value.order_id", "value.order_status", "value.order_purchase_timestamp") \
    .withColumn("order_receive_time", F.current_timestamp()) \
    .withColumn("window_time",F.window(F.col("order_receive_time"),"2 minutes")) \
    .withWatermark("window_time", "2 minutes")
    
watermarked_windowed_orders.printSchema()
```

    root
     |-- order_id: string (nullable = true)
     |-- order_status: string (nullable = true)
     |-- order_purchase_timestamp: string (nullable = true)
     |-- order_receive_time: timestamp (nullable = false)
     |-- window_time: struct (nullable = false)
     |    |-- start: timestamp (nullable = true)
     |    |-- end: timestamp (nullable = true)


Делаем джойн двух датасетов. 

```python
streams_joined = watermarked_windowed_orders \
    .join(extended_orders_items, ["order_id", "window_time"] , "inner") \
    .select("order_id", "order_item_id", "product_id", "window_time")
```

Тип отображения `update`не подходит для `inner` джойна.
`hdfs dfs -rm -r -skipTrash checkpoints`

это очень медленно
```python
stream = console_output(streams_joined , 20, "append")
stream.stop()
```

здесь нет данных, так как видимо по сдвоенному ключу совпадений нет

    -------------------------------------------
    Batch: 0
    -------------------------------------------
    +--------+-------------+----------+-----------+
    |order_id|order_item_id|product_id|window_time|
    +--------+-------------+----------+-----------+
    +--------+-------------+----------+-----------+

    23/03/15 08:47:38 WARN streaming.ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 20000 milliseconds, but spent 162681 milliseconds
    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +--------+-------------+----------+-----------+
    |order_id|order_item_id|product_id|window_time|
    +--------+-------------+----------+-----------+
    +--------+-------------+----------+-----------+

даже если задать один ключ без временного окна (допускается для этого режима) - совпадения придётся ждать очень-очень долго (много уникальных + достаточно много данных + данные перемешаны) - к 14 батчу (каждый около 100000 мсек) ещё нет совпадений
```python
streams_joined = watermarked_windowed_orders \
    .join(extended_orders_items, ["order_id"] , "inner") \
    .select("order_id", "order_item_id", "product_id")

stream = console_output(streams_joined , 20, "append")
stream.stop()
```
