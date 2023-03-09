##### Задание 1. Повторить чтение файлов со своими файлами со своей схемой.


- подключение

```bash
$ ssh raj_ops@127.0.0.1 -p 2222
```

- файлы и папки

```bash
[raj_ops@sandbox-hdp ~]$ hdfs dfs -mkdir input_csv_for_stream
[raj_ops@sandbox-hdp ~]$ mkdir for_stream
```

- копирование файлов на удалённый сервер

```bash
013@WINCTRL-GG59FTL MINGW64 /
$ scp -P 2222 -r '/d/docs/GB/Потоковая обработка данных/lesson3/for_stream/' raj_ops@127.0.0.1:/home/raj_ops/
raj_ops@127.0.0.1's password:
id_name_translation1.csv                                       100%  336    56.8KB/s   00:00
id_name_translation2.csv                                       100%  330    27.9KB/s   00:00
id_name_translation3.csv                                       100%  490   127.0KB/s   00:00
id_name_translation4.csv                                       100%  417   106.8KB/s   00:00

```

```bash
[raj_ops@sandbox-hdp ~]$ pyspark
```

- импорты и функция для вывода

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType

def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=False) \
        .start()
```

- схема файлов
```python
schema = StructType() \
    .add('id', IntegerType()) \
    .add('product_category_name', StringType()) \
    .add('product_category_name_english', StringType())
```
 
- создание потока
```python
raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .options(path="input_csv_for_stream", header=True) \
    .load()
out = console_output(raw_files, 5)
```


- 1 файл в горячей папке
```bash
[raj_ops@sandbox-hdp for_stream]$ hdfs dfs -put id_name_translation1.csv input_csv_for_stream
```

В первом терминале - чтение данных. 

    -------------------------------------------
    Batch: 0
    -------------------------------------------
    +---+----------------------+-----------------------------+
    |id |product_category_name |product_category_name_english|
    +---+----------------------+-----------------------------+
    |0  |beleza_saude          |health_beauty                |
    |1  |informatica_acessorios|computers_accessories        |
    |2  |automotivo            |auto                         |
    |3  |cama_mesa_banho       |bed_bath_table               |
    |4  |moveis_decoracao      |furniture_decor              |
    |5  |esporte_lazer         |sports_leisure               |
    |6  |perfumaria            |perfumery                    |
    |7  |utilidades_domesticas |housewares                   |
    |8  |telefonia             |telephony                    |
    +---+----------------------+-----------------------------+

    23/03/07 10:10:06 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 5000 milliseconds, but spent 5905 milliseconds



Во втором терминале - все оставшиеся файлы на HDFS.
```bash
[raj_ops@sandbox-hdp for_stream]$ hdfs dfs -put id_name_translation* input_csv_for_stream
[raj_ops@sandbox-hdp for_stream]$ hdfs dfs -ls input_csv_for_stream
Found 4 items
-rw-r--r--   1 raj_ops hdfs        336 2023-03-07 08:44 input_csv_for_stream/id_name_translation1.csv
-rw-r--r--   1 raj_ops hdfs        330 2023-03-07 08:44 input_csv_for_stream/id_name_translation2.csv
-rw-r--r--   1 raj_ops hdfs        490 2023-03-07 08:44 input_csv_for_stream/id_name_translation3.csv
-rw-r--r--   1 raj_ops hdfs        417 2023-03-07 08:44 input_csv_for_stream/id_name_translation4.csv

```

Вывод из 1 терминала


    Batch: 1
    -------------------------------------------
    +---+----------------------------------------------+---------------------------------------+
    |id |product_category_name                         |product_category_name_english          |
    +---+----------------------------------------------+---------------------------------------+
    |17 |malas_acessorios                              |luggage_accessories                    |
    |18 |climatizacao                                  |air_conditioning                       |
    |19 |construcao_ferramentas_construcao             |construction_tools_construction        |
    |20 |moveis_cozinha_area_de_servico_jantar_e_jardim|kitchen_dining_laundry_garden_furniture|
    |21 |construcao_ferramentas_jardim                 |costruction_tools_garden               |
    |22 |fashion_roupa_masculina                       |fashion_male_clothing                  |
    |23 |pet_shop                                      |pet_shop                               |
    |24 |moveis_escritorio                             |office_furniture                       |
    |25 |market_place                                  |market_place                           |
    |26 |eletrodomesticos                              |home_appliances                        |
    |27 |artigos_de_festas                             |party_supplies                         |
    |28 |casa_conforto                                 |home_confort                           |
    |29 |construcao_ferramentas_ferramentas            |costruction_tools_tools                |
    |30 |agro_industria_e_comercio                     |agro_industry_and_commerce             |
    |31 |moveis_colchao_e_estofado                     |furniture_mattress_and_upholstery      |
    |32 |livros_tecnicos                               |books_technical                        |
    |33 |casa_construcao                               |home_construction                      |
    |9  |bebes                                         |baby                                   |
    |10 |papelaria                                     |stationery                             |
    |11 |tablets_impressao_imagem                      |tablets_printing_image                 |
    +---+----------------------------------------------+---------------------------------------+
    only showing top 20 rows

- завершение стрима

```python
out.stop()
```

- вывод по 1 файлу за раз + 2 новых столбца

```python
raw_files = spark \
    .readStream \
    .format('csv') \
    .schema(schema) \
    .options(path='input_csv_for_stream',
             header=True,
             maxFilesPerTrigger=1) \
    .load()
extra_files = raw_files \
    .withColumn('diff_len', F.length(F.col('product_category_name')) - F.length(F.col('product_category_name_english'))) \
    .withColumn('diff', F.when(F.col('diff_len') > 0, '+').when(F.col('diff_len') < 0, '-').otherwise('='))
out = console_output(extra_files, 5)
```

- первый и последний батчи

-------------------------------------------
    Batch: 0
    -------------------------------------------
    +---+----------------------+-----------------------------+--------+----+
    |id |product_category_name |product_category_name_english|diff_len|diff|
    +---+----------------------+-----------------------------+--------+----+
    |0  |beleza_saude          |health_beauty                |-1      |-   |
    |1  |informatica_acessorios|computers_accessories        |1       |+   |
    |2  |automotivo            |auto                         |6       |+   |
    |3  |cama_mesa_banho       |bed_bath_table               |1       |+   |
    |4  |moveis_decoracao      |furniture_decor              |1       |+   |
    |5  |esporte_lazer         |sports_leisure               |-1      |-   |
    |6  |perfumaria            |perfumery                    |1       |+   |
    |7  |utilidades_domesticas |housewares                   |11      |+   |
    |8  |telefonia             |telephony                    |0       |=   |
    +---+----------------------+-----------------------------+--------+----+

...

    -------------------------------------------
    Batch: 3
    -------------------------------------------
    +---+----------------------------------+---------------------------------+--------+----+
    |id |product_category_name             |product_category_name_english    |diff_len|diff|
    +---+----------------------------------+---------------------------------+--------+----+
    |26 |eletrodomesticos                  |home_appliances                  |1       |+   |
    |27 |artigos_de_festas                 |party_supplies                   |3       |+   |
    |28 |casa_conforto                     |home_confort                     |1       |+   |
    |29 |construcao_ferramentas_ferramentas|costruction_tools_tools          |11      |+   |
    |30 |agro_industria_e_comercio         |agro_industry_and_commerce       |-1      |-   |
    |31 |moveis_colchao_e_estofado         |furniture_mattress_and_upholstery|-8      |-   |
    |32 |livros_tecnicos                   |books_technical                  |0       |=   |
    |33 |casa_construcao                   |home_construction                |-2      |-   |
    +---+----------------------------------+---------------------------------+--------+----+

cnhjrf

```python
out.stop()
exit()
```

##### Задание 2. Создать свой топик/топики, загрузить туда через консоль осмысленные данные с kaggle. Лучше в формате json. Много сообщений не нужно, достаточно штук 10-100. Прочитать свой топик так же, как на уроке.

- скрипт python для построчного вывода json
(и это не оптимально, да)
```python
#!/bin/python3.6
from sys import argv
import json

my_file = argv[1]

with open(my_file, 'r') as f:
        my_json = json.load(f)

for el in my_json:
        print(el)

```

во 2 консоли консьюмер
- создаём топик и заполняем его из .json
```bash
[raj_ops@sandbox-hdp ~]$ /usr/hdp/3.0.1.0-187/kafka/bin/kafka-topics.sh --create --topic lesson3 --zookeeper localhost:2181 --partitions 3 --replication-factor 1 --config retention.ms=-1
[raj_ops@sandbox-hdp ~]$ ./json_reader.py for_stream/OnePiece.json | /usr/hdp/3.0.1.0-187/kafka/bin/kafka-console-producer.sh --topic lesson3 --broker-list sandbox-hdp.hortonworks.com:6667
```
- консьюмером для проверки читаем 3 строки
```bash
[raj_ops@sandbox-hdp ~]$ /usr/hdp/3.0.1.0-187/kafka/bin/kafka-console-consumer.sh --topic lesson3 --bootstrap-server sandbox-hdp.hortonworks.com:6667 --from-beginning --max-messages 3
{'Unnamed: 0': 2, 'rank': '32,043', 'trend': '7', 'season': 1, 'episode': 3, 'name': "Morgan vs. Luffy! Who's This Beautiful Young Girl?", 'start': 1999, 'total_votes': '428', 'average_rating': 7.7}
{'Unnamed: 0': 5, 'rank': '36,209', 'trend': '4', 'season': 1, 'episode': 6, 'name': 'Desperate Situation! Beast Tamer Mohji vs. Luffy!', 'start': 1999, 'total_votes': '364', 'average_rating': 7.7}
{'Unnamed: 0': 8, 'rank': '42,249', 'trend': '5', 'season': 1, 'episode': 9, 'name': 'Seigi no usotsuki? Kyaputen Usoppu', 'start': 2000, 'total_votes': '327', 'average_rating': 7.3}
Processed a total of 3 messages
```

- в 1 консоли - pyspark с кафкой
```bash
[raj_ops@sandbox-hdp ~]$ pyspark --master local[1] --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2
```
```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType

def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=True) \
        .start()

raw_data = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667") \
    .option("subscribe", "lesson3") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", """{"lesson3":{"0":20,"1":-1,"2":-1}}""") \
    .load()

raw_data.printSchema()
```
    root
     |-- key: binary (nullable = true)
     |-- value: binary (nullable = true)
     |-- topic: string (nullable = true)
     |-- partition: integer (nullable = true)
     |-- offset: long (nullable = true)
     |-- timestamp: timestamp (nullable = true)
     |-- timestampType: integer (nullable = true)

```python
raw_data.show(1)
```
    +----+--------------------+-------+---------+------+--------------------+-------------+
    | key|               value|  topic|partition|offset|           timestamp|timestampType|
    +----+--------------------+-------+---------+------+--------------------+-------------+
    |null|[7B 27 55 6E 6E 6...|lesson3|        0|     0|2023-03-09 04:24:...|            0|
    +----+--------------------+-------+---------+------+--------------------+-------------+
only showing top 1 row


- далее чтение в потоке
```python
raw_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667") \
    .option("subscribe", "lesson3") \
    .option("startingOffsets", "earliest") \
    .load()
out = console_output(raw_data, 5)
out.stop()
```
    Batch: 0
    -------------------------------------------
    +----+--------------------+-------+---------+------+--------------------+-------------+
    | key|               value|  topic|partition|offset|           timestamp|timestampType|
    +----+--------------------+-------+---------+------+--------------------+-------------+
    |null|[7B 27 55 6E 6E 6...|lesson3|        1|     0|2023-03-09 04:24:...|            0|
    |null|[7B 27 55 6E 6E 6...|lesson3|        1|     1|2023-03-09 04:24:...|            0|
    |null|[7B 27 55 6E 6E 6...|lesson3|        1|     2|2023-03-09 04:24:...|            0|
    |null|[7B 27 55 6E 6E 6...|lesson3|        1|     3|2023-03-09 04:24:...|            0|

- то же самое, но по 5
```python
raw_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667") \
    .option("subscribe", "lesson3") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "5") \
    .load()
out = console_output(raw_data, 5)
out.stop()
```

    Batch: 0
    -------------------------------------------
    +----+--------------------+-------+---------+------+--------------------+-------------+
    | key|               value|  topic|partition|offset|           timestamp|timestampType|
    +----+--------------------+-------+---------+------+--------------------+-------------+
    |null|[7B 27 55 6E 6E 6...|lesson3|        1|     0|2023-03-09 04:24:...|            0|
    |null|[7B 27 55 6E 6E 6...|lesson3|        0|     0|2023-03-09 04:24:...|            0|
    |null|[7B 27 55 6E 6E 6...|lesson3|        2|     0|2023-03-09 04:24:...|            0|
    +----+--------------------+-------+---------+------+--------------------+-------------+

    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +----+--------------------+-------+---------+------+--------------------+-------------+
    | key|               value|  topic|partition|offset|           timestamp|timestampType|
    +----+--------------------+-------+---------+------+--------------------+-------------+
    |null|[7B 27 55 6E 6E 6...|lesson3|        1|     1|2023-03-09 04:24:...|            0|
    |null|[7B 27 55 6E 6E 6...|lesson3|        0|     1|2023-03-09 04:24:...|            0|
    |null|[7B 27 55 6E 6E 6...|lesson3|        2|     1|2023-03-09 04:24:...|            0|
    +----+--------------------+-------+---------+------+--------------------+-------------+

- только новые поступления
```python
raw_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667") \
    .option("subscribe", "lesson3") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "5") \
    .load()
out = console_output(raw_data, 5)
out.stop()
```

- произвольный offset
```python
raw_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667") \
    .option("subscribe", "lesson3") \
    .option("startingOffsets", """{"lesson3":{"0":20,"1":-1,"2":-1}}""") \
    .option("maxOffsetsPerTrigger", "5") \
    .load()
out = console_output(raw_data, 5)
out.stop()
```
     -------------------------------------------
    Batch: 0
    -------------------------------------------
    +----+--------------------+-------+---------+------+--------------------+-------------+
    | key|               value|  topic|partition|offset|           timestamp|timestampType|
    +----+--------------------+-------+---------+------+--------------------+-------------+
    |null|[7B 27 55 6E 6E 6...|lesson3|        0|    20|2023-03-09 04:24:...|            0|
    |null|[7B 27 55 6E 6E 6...|lesson3|        0|    21|2023-03-09 04:24:...|            0|
    |null|[7B 27 55 6E 6E 6...|lesson3|        0|    22|2023-03-09 04:24:...|            0|
    |null|[7B 27 55 6E 6E 6...|lesson3|        0|    23|2023-03-09 04:24:...|            0|
    |null|[7B 27 55 6E 6E 6...|lesson3|        0|    24|2023-03-09 04:24:...|            0|
    +----+--------------------+-------+---------+------+--------------------+-------------+


- структура данных исходного датасета.

```python
schema = StructType() \
    .add('Unnamed: 0', StringType()) \
    .add('rank', StringType()) \
    .add('trend', StringType()) \
    .add('episode', StringType()) \
    .add('name', StringType()) \
    .add('start', StringType()) \
    .add('total_votes', StringType()) \
    .add('average_rating', StringType())

value_OnePiece = raw_data \
    .select(
        F.from_json(F.col("value").cast("String"), schema).alias("value"), 
        "offset"
    )

value_OnePiece.printSchema()
```
    root
     |-- value: struct (nullable = true)
     |    |-- Unnamed: 0: string (nullable = true)
     |    |-- rank: string (nullable = true)
     |    |-- trend: string (nullable = true)
     |    |-- episode: string (nullable = true)
     |    |-- name: string (nullable = true)
     |    |-- start: string (nullable = true)
     |    |-- total_votes: string (nullable = true)
     |    |-- average_rating: string (nullable = true)
     |-- offset: long (nullable = true)

```python
parsed_OnePiece = value_OnePiece.select("value.*", "offset")
parsed_OnePiece.printSchema()
```
    root
     |-- Unnamed: 0: string (nullable = true)
     |-- rank: string (nullable = true)
     |-- trend: string (nullable = true)
     |-- episode: string (nullable = true)
     |-- name: string (nullable = true)
     |-- start: string (nullable = true)
     |-- total_votes: string (nullable = true)
     |-- average_rating: string (nullable = true)
     |-- offset: long (nullable = true)

- приведение типов
```python
parsed_typed_OnePiece = parsed_OnePiece.select(
                             F.col('Unnamed: 0').cast('Integer').alias('Unnamed: 0'),
                             F.regexp_replace(F.col('rank'), ',', '.').cast('Float').alias('rank'),
                             F.col('trend').cast('Integer').alias('trend'),
                             F.col('episode').cast('Integer').alias('episode'),
                             F.col('name'),
                             F.col('start').cast('Integer').alias('start'),
                             F.col('total_votes').cast('Integer').alias('total_votes'),
                             F.col('average_rating').cast('Float').alias('average_rating'),
                             F.col('offset')
                            )
```
```python
out = console_output(parsed_typed_OnePiece, 5)
out.stop()
```
    -------------------------------------------
    Batch: 0
    -------------------------------------------
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+
    |Unnamed: 0|  rank|trend|episode|                name|start|total_votes|average_rating|offset|
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+
    |        62|56.518|    1|     63|Otoko no Yakusoku...| 2001|        216|           7.4|    20|
    |        65|51.563|    1|     66|Shinken Shoubu! L...| 2001|        214|           7.9|    21|
    |        68| 66.94|    1|     69|Coby-Meppo no Ket...| 2001|        202|           6.8|    22|
    |        71|60.685|    1|     72|Luffy Ikaru! Sein...| 2001|        202|           7.3|    23|
    |        74| 57.02|    1|     75|Luffy o Osou Mary...| 2001|        197|           7.7|    24|
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+

    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+
    |Unnamed: 0|  rank|trend|episode|                name|start|total_votes|average_rating|offset|
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+
    |        77|60.499|    1|     78|Nami ga Byouki? U...| 2001|        203|           7.3|    25|
    |        80|58.903|    1|     81|Happy kai? Majo t...| 2001|        189|           7.7|    26|
    |        83|56.392|    1|     84|Tonakai wa Aoppan...| 2001|        200|           7.7|    27|
    |        86| 55.48|    1|     87|Versus Wapol Gund...| 2001|        194|           7.9|    28|
    |        89|48.464|    1|     90|Hiluluk no Sakura...| 2001|        211|           8.3|    29|
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+

- фильтрация
```python
filtered_OnePiece = parsed_typed_OnePiece.filter(F.col('rank') > 60)
out = console_output(filtered_OnePiece, 5)
out.stop()
```
    Batch: 0
    -------------------------------------------
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+
    |Unnamed: 0|  rank|trend|episode|                name|start|total_votes|average_rating|offset|
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+
    |        68| 66.94|    1|     69|Coby-Meppo no Ket...| 2001|        202|           6.8|    22|
    |        71|60.685|    1|     72|Luffy Ikaru! Sein...| 2001|        202|           7.3|    23|
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+

    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+
    |Unnamed: 0|  rank|trend|episode|                name|start|total_votes|average_rating|offset|
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+
    |        77|60.499|    1|     78|Nami ga Byouki? U...| 2001|        203|           7.3|    25|
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+

    -------------------------------------------
    Batch: 2
    -------------------------------------------
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+
    |Unnamed: 0|  rank|trend|episode|                name|start|total_votes|average_rating|offset|
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+
    |        92| 64.16|    1|     93|Iza Sabaku no Kun...| 2001|        192|           7.2|    30|
    |        95|62.235|    1|     96|Midori no Machi E...| 2002|        195|           7.3|    31|
    |        98|68.255|    1|     99|Nisemono no Iji! ...| 2002|        180|           7.1|    32|
    |       101|71.389|    1|    102|Iseki to Mayoigo!...| 2002|        168|           7.1|    33|
    |       104|60.105|    1|    105|Alabasta Sensen! ...| 2002|        175|           7.9|    34|
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+

- вывод с чекпоинтами
```python
def console_output_checkpointed(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq) \
        .option("truncate", True) \
        .option("checkpointLocation", "OnePiece_console_checkpoint") \
        .start()

out = console_output_checkpointed(parsed_typed_OnePiece, 5)
out.stop()
```
- завершили на 34


    Batch: 2
-------------------------------------------
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+
    |Unnamed: 0|  rank|trend|episode|                name|start|total_votes|average_rating|offset|
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+
    |        92| 64.16|    1|     93|Iza Sabaku no Kun...| 2001|        192|           7.2|    30|
    |        95|62.235|    1|     96|Midori no Machi E...| 2002|        195|           7.3|    31|
    |        98|68.255|    1|     99|Nisemono no Iji! ...| 2002|        180|           7.1|    32|
    |       101|71.389|    1|    102|Iseki to Mayoigo!...| 2002|        168|           7.1|    33|
    |       104|60.105|    1|    105|Alabasta Sensen! ...| 2002|        175|           7.9|    34|
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+
```python
out = console_output_checkpointed(parsed_typed_OnePiece, 5)
out.stop()
```
- начали с 35
    Batch: 3
-------------------------------------------
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+
    |Unnamed: 0|  rank|trend|episode|                name|start|total_votes|average_rating|offset|
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+
    |       107|56.555|    1|    108|Kyoufu no Bananaw...| 2002|        180|           8.1|    35|
    |       110|59.499|    1|    111|Kiseki e no Shiss...| 2002|        182|           7.8|    36|
    |       113| 58.35|    3|    114|Nakama no Yume ni...| 2002|        182|           7.9|    37|
    |       116| 57.41|    3|    117|Nami no Senpuu Ch...| 2002|        181|           8.0|    38|
    |       119|59.563|    2|    120|Tatakai wa Owatta...| 2002|        177|           7.9|    39|
    +----------+------+-----+-------+--------------------+-----+-----------+--------------+------+

- в другой консоли - папка с данными о чекпоинтах
```bash
[raj_ops@sandbox-hdp ~]$ hdfs dfs -ls OnePiece_console_checkpoint
Found 4 items
drwxr-xr-x   - raj_ops hdfs          0 2023-03-09 11:27 OnePiece_console_checkpoint/commits
-rw-r--r--   1 raj_ops hdfs         45 2023-03-09 11:27 OnePiece_console_checkpoint/metadata
drwxr-xr-x   - raj_ops hdfs          0 2023-03-09 11:27 OnePiece_console_checkpoint/offsets
drwxr-xr-x   - raj_ops hdfs          0 2023-03-09 11:27 OnePiece_console_checkpoint/sources
```
