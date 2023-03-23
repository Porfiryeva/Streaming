## Урок 7. Spark Submit. Lambda архитектура.


```bash
[raj_ops@sandbox-hdp ~]$ /spark2.4.8/spark-2.4.8-bin-hadoop2.7/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2
```

- создаём датасет
```python
my_df = spark.createDataFrame(range(1, 200000), IntegerType())
items_df = my_df.select(F.col("value").alias("order_id"),
                        F.round((F.rand() * 49999) + 1).alias("user_id").cast("integer"),
                        F.round((F.rand() * 9) + 1).alias("items_count").cast("integer")) \
            .withColumn("price", (F.col("items_count") * F.round((F.rand() * 999) + 1)).cast("integer")) \
            .withColumn("order_date", F.from_unixtime(F.unix_timestamp(F.current_date()) + 
                (F.lit(F.col("order_id") * 10))))

items_df.show(5)
```

    23/03/23 09:10:32 WARN scheduler.TaskSetManager: Stage 1 contains a task of very large size (272 KB). The maximum recommended task size is 100 KB.
    +--------+-------+-----------+-----+-------------------+
    |order_id|user_id|items_count|price|         order_date|
    +--------+-------+-----------+-----+-------------------+
    |       1|   5825|          2| 1092|2023-03-23 00:00:10|
    |       2|    724|          5| 3490|2023-03-23 00:00:20|
    |       3|  47309|          2|   14|2023-03-23 00:00:30|
    |       4|  28704|          4| 2404|2023-03-23 00:00:40|
    |       5|  30947|          4| 3508|2023-03-23 00:00:50|
    +--------+-------+-----------+-----+-------------------+
    only showing top 5 rows

- сохраняем датасет как файл и как таблицу в Hive
```python
spark.sql("CREATE DATABASE sint_sales")
items_df.write.format("parquet").option("path", "parquet_data/sales").saveAsTable("sint_sales.sales", mode="overwrite")
spark.read.parquet("parquet_data/sales/").show(1)
spark.sql("SELECT * FROM sint_sales.sales").show(1)
```
    +--------+-------+-----------+-----+-------------------+
    |order_id|user_id|items_count|price|         order_date|
    +--------+-------+-----------+-----+-------------------+
    |   66561|  16739|          4| 3068|2023-03-30 16:53:30|
    +--------+-------+-----------+-----+-------------------+
    only showing top 1 row

- считываем из таблицы
```python
items_df = spark.table("sint_sales.sales")
```

- создаём  и заполняем ещё одну таблицу
```python
spark.sql("""CREATE TABLE sint_sales.users(
    user_id int,
    gender string,
    age string,
    segment string) 
stored as parquet location 'parquet_data/users'""")

spark.sql("""INSERT into sint_sales.users 
    SELECT user_id, CASE WHEN pmod(user_id, 2)=0 THEN 'M' ELSE 'F' END,
    CASE WHEN pmod(user_id, 3)=0 then 'young' when pmod(user_id, 3)=1 THEN 'midage' ELSE 'old' END,
    CASE WHEN s>23 THEN 'happy' WHEN s>15 THEN 'neutral' ELSE 'shy' END
    FROM (
        SELECT SUM(items_count) s, user_id from sint_sales.sales GROUP BY user_id) t""")
```

- разбиваем датасеты
```python
spark.sql("""CREATE TABLE sint_sales.users_known STORED as parquet location 'parquet_data/users_known' as
    SELECT * FROM sint_sales.users WHERE user_id < 30000""")

spark.sql("""CREATE TABLE sint_sales.users_unknown STORED as parquet location 'parquet_data/users_unknown' as
    SELECT user_id, gender, age FROM sint_sales.users WHERE user_id >= 30000""")

spark.sql("""CREATE TABLE sint_sales.sales_known STORED as parquet location 'parquet_data/sales_known' as
    SELECT * FROM sint_sales.sales WHERE user_id < 30000""")

spark.sql("""CREATE TABLE sint_sales.sales_unknown STORED as parquet location 'parquet_data/sales_unknown' as
    SELECT * FROM sint_sales.sales WHERE user_id >= 30000""")
```

- импорты
```python
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler, CountVectorizer, StringIndexer, IndexToString
```

- датасет из двух таблиц - аггрегаты по user_id
    - число заказов
    - общее число товаров
    - максимум товаров за 1 заказ
    - минимум товаров за 1 по купку
    - общая сумма покупок
    - max чек
    - min чек
- задача - предсказать сегмент
```python
users_known = spark.sql("""
select count(*) as c, sum(items_count) as s1, max(items_count) as ma1, min(items_count) as mi1,
sum(price) as s2, max(price) as ma2, min(price) as mi2 ,u.gender, u.age, u.user_id, u.segment 
from sint_sales.sales_known s join sint_sales.users_known u 
where s.user_id = u.user_id 
group by u.user_id, u.gender, u.age, u.segment""")

users_known.show(1)
```

    +---+---+---+---+----+----+----+------+------+-------+-------+
    |  c| s1|ma1|mi1|  s2| ma2| mi2|gender|   age|user_id|segment|
    +---+---+---+---+----+----+----+------+------+-------+-------+
    |  3| 19|  9|  4|7280|2988|1328|     M|midage|    148|neutral|
    +---+---+---+---+----+----+----+------+------+-------+-------+
    only showing top 1 row

- кодирование категориальных данных
```python
categoricalColumns = ['gender', 'age']
stages = []
for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
    encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
    stages += [stringIndexer, encoder]

stages
```
    [StringIndexer_cad8ee56926b, OneHotEncoderEstimator_bea90a1dfe66, StringIndexer_2bf104130182, OneHotEncoderEstimator_539ae6d722af]

- кодирование таргета
```python
label_stringIdx = StringIndexer(inputCol = 'segment', outputCol = 'label')
stages += [label_stringIdx]
```

- создание вектора признаков
```python
numericCols = ['c' ,'s1', 'ma1', 'mi1','s2', 'ma2', 'mi2']
assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]
```

- добавление ML модели
```python
lr = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=10)
stages += [lr]

stages
```
    [StringIndexer_cad8ee56926b, OneHotEncoderEstimator_bea90a1dfe66, StringIndexer_2bf104130182, OneHotEncoderEstimator_539ae6d722af, StringIndexer_81fa36f01cf6,  VectorAssembler_f5fadc1bfbf1, LogisticRegression_a865297d5a77]

- добавляем в пайплайн преобразователь для декодирования таргета
```python
label_stringIdx_fit = label_stringIdx.fit(users_known)
indexToStringEstimator = IndexToString().setInputCol("prediction").setOutputCol("category").setLabels(label_stringIdx_fit.labels)

stages +=[indexToStringEstimator]
```

- создаём и обучаем пайплайн, сохраняем модель и получаем предсказание
```python
pipeline = Pipeline().setStages(stages)
pipelineModel = pipeline.fit(users_known)

pipelineModel.write().overwrite().save("my_LR_model")

result_df = pipelineModel.transform(users_known)
result_df.show(1, vertical=True)
```
    -RECORD 0------------------------------
     c              | 3
     s1             | 19
     ma1            | 9
     mi1            | 4
     s2             | 7280
     ma2            | 2988
     mi2            | 1328
     gender         | M
     age            | midage
     user_id        | 148
     segment        | neutral
     genderIndex    | 0.0
     genderclassVec | (1,[0],[1.0])
     ageIndex       | 2.0
     ageclassVec    | (2,[],[])
     label          | 2.0
     features       | [1.0,0.0,0.0,3.0,...
     rawPrediction  | [-1.5472373743120...
     probability    | [0.04679717837671...
     prediction     | 2.0
     category       | neutral
    only showing top 1 row

```python
result_df.select('segment', 'label', 'prediction', 'category').show(5)
```
    +-------+-----+----------+--------+
    |segment|label|prediction|category|
    +-------+-----+----------+--------+
    |neutral|  2.0|       2.0| neutral|
    |  happy|  0.0|       0.0|   happy|
    |neutral|  2.0|       1.0|     shy|
    |  happy|  0.0|       2.0| neutral|
    |    shy|  1.0|       1.0|     shy|
    +-------+-----+----------+--------+


##### Задание. Повторить запуск Spark приложений с такими параметрами (можно еще добавлять свои):
 
 ```bash
 /spark2.4/bin/spark-submit --driver-memory 512m --driver-cores 1 --master local[1] my_script.py
```

Для начала скопируем все исходники в домашнюю директорию. 

```bash
$ scp -P 2222 7.1_spark-submit_stream.py 7.2_spark-submit_stable.py 7.spark-submit-batch.py raj_ops@127.0.0.1:/home/raj_ops/
raj_ops@127.0.0.1's password:
7.1_spark-submit_stream.py                    100% 1379   115.7KB/s   00:00
7.2_spark-submit_stable.py                    100% 1677   423.5KB/s   00:00
7.spark-submit-batch.py   
```

Подключаемся к серверу.

```bash
$ ssh raj_ops@127.0.0.1 -p 2222 
```

Копируем файлы, которые будут использованы как исходники для стрима, на HDFS.

```bash
[raj_ops@sandbox-hdp ~]$ hdfs dfs -mkdir for_stream
[raj_ops@sandbox-hdp ~]$ hdfs dfs -put for_stream/* for_stream
```

Запускаем скрипт батчевой обработки csv-файлов. Сделаем это несколько раз.

 ```bash
[raj_ops@sandbox-hdp ~]$ /spark2.4.8/spark-2.4.8-bin-hadoop2.7/bin/spark-submit --driver-memory 512m --driver-cores 1 --master local[1] 7.spark-submit-batch.py
```

Видим что скрипт успешно отработал три раза и при каждом запуске был создан паркет-файл в директории `my_submit_parquet_files` на HDFS.

```bash
[raj_ops@sandbox-hdp ~]$ hdfs dfs -ls my_submit_parquet_files
Found 3 items
drwxr-xr-x   - raj_ops hdfs          0 2023-03-23 11:45 my_submit_parquet_files/p_date=20230323114527
drwxr-xr-x   - raj_ops hdfs          0 2023-03-23 11:46 my_submit_parquet_files/p_date=20230323114643
drwxr-xr-x   - raj_ops hdfs          0 2023-03-23 11:47 my_submit_parquet_files/p_date=20230323114704
```

Запустим второй скрипт. Тут чтение файлов происходит в стриминговом режиме раз в 10 секунд. Ограничений на количество файлов нет, чейкпойнт не указан.  

```bash
[raj_ops@sandbox-hdp ~]$ /spark2.4.8/spark-2.4.8-bin-hadoop2.7/bin/spark-submit --driver-memory 512m --driver-cores 1 --master local[1] 7.1_spark-submit_stream.py
```

Скрипт отработал и завершился, так как был достигнут конец файла. Микробатч ни разу не отработал. В списке файлов появился пустой файл. 

```bash
[raj_ops@sandbox-hdp ~]$ hdfs dfs -du -h my_submit_parquet_files
11.0 M  11.0 M  my_submit_parquet_files/p_date=20230323114527
11.0 M  11.0 M  my_submit_parquet_files/p_date=20230323114643
11.0 M  11.0 M  my_submit_parquet_files/p_date=20230323114704
0       0       my_submit_parquet_files/p_date=20230323115106
```

Запустим третий скрипт. 

```bash
[raj_ops@sandbox-hdp ~]$ /spark2.4.8/spark-2.4.8-bin-hadoop2.7/bin/spark-submit --driver-memory 512m --driver-cores 1 --master local[1] 7.2_spark-submit_stable.py
```

Скрипт не отпускает консоль. раз в 10 секунд пишется сообщение о новом микробатче. Раз в 9 секунд выводится текст `I'M STILL ALIVE`. Проверим что все файлы успено прочитались:

    I'M STILL ALIVE
    23/03/23 11:54:30 INFO datasources.InMemoryFileIndex: It took 28 ms to list leaf files for 1 paths.
    I'M STILL ALIVE


```bash
[raj_ops@sandbox-hdp ~]$ hdfs dfs -du -h my_submit_parquet_files
11.0 M  11.0 M  my_submit_parquet_files/p_date=20230323114527
11.0 M  11.0 M  my_submit_parquet_files/p_date=20230323114643
11.0 M  11.0 M  my_submit_parquet_files/p_date=20230323114704
0       0       my_submit_parquet_files/p_date=20230323115106
11.0 M  11.0 M  my_submit_parquet_files/p_date=20230323115417
``````

<details>
<summary>Для истории. Пример запуска spark-submit с параметрами.</summary>

```
/spark2.4.8/spark-2.4.8-bin-hadoop2.7/bin/spark-submit --conf spark.hadoop.hive.exec.max.dynamic.partitions=10000 \
--conf spark.hadoop.hive.exec.max.dynamic.partitions.pernode=3000 \
--conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict \
--conf spark.hadoop.hive.eror.on.empty.partition=true \
--conf spark.hadoop.hive.exec.dynamic.partition=true \
--conf spark.sql.parquet.compression.codec=gzip \
--conf spark.sql.catalogImplementation=hive \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryoserializer.buffer=128M \
--conf spark.kryoserializer.buffer.max=2000M \
--conf spark.sql.broadcastTimeout=6000 \
--conf spark.network.timeout=600s \
--conf spark.driver.memory=20g \
--conf spark.driver.memoryOverhead=3g \
--conf spark.executor.memory=20g \
--conf spark.executor.memoryOverhead=3g \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.sql.shuffle.partitions=300 \
--conf spark.shuffle.service.enabled=true \
test_submit.py
```
```bash
[raj_ops@sandbox-hdp ~]$ hdfs dfs -du -h my_submit_parquet_files
1.6 K   1.6 K   my_submit_parquet_files/_spark_metadata
11.0 M  11.0 M  my_submit_parquet_files/p_date=20230323114527
11.0 M  11.0 M  my_submit_parquet_files/p_date=20230323114643
11.0 M  11.0 M  my_submit_parquet_files/p_date=20230323114704
0       0       my_submit_parquet_files/p_date=20230323115106
11.0 M  11.0 M  my_submit_parquet_files/p_date=20230323115417
891     891     my_submit_parquet_files/part-00000-0813e1a2-7d66-4255-bf3e-ac3fb7ccacc4-c000.gz.parquet
891     891     my_submit_parquet_files/part-00001-1ad7ef23-26ad-4652-8b04-1b3a4fd64720-c000.gz.parquet
891     891     my_submit_parquet_files/part-00002-c0725d8c-775c-4d7a-aa35-012e590f74aa-c000.gz.parquet
891     891     my_submit_parquet_files/part-00003-aa8f8f18-3bae-4a7c-a483-f10bc19b1bba-c000.gz.parquet
1.5 K   1.5 K   my_submit_parquet_files/part-00004-39846da9-2656-4e42-be3b-985f4ece940b-c000.gz.parquet
1.4 K   1.4 K   my_submit_parquet_files/part-00005-4fe52dc3-bc9a-4076-92a9-1410a4068d64-c000.gz.parquet
```
</details>
