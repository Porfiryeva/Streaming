# Итоговый проект курса "Потоковая обработка данных"

- Стек:
    - среда - Hortonworks Sandbox HDP 3.0 с локально установленным Spark-2.4.8
    - Cassandra, Kafka, Spark Streaming, Spark ML,
    - библиотеки: pandas, pyspark

#### 1. Постановка задачи

- на HDFS хранится справочная информация о участках дорог (roads)
- имеются исторические данные о трафике на участках дорог и данные о авариях (хранятся на HDFS - traffic_for_ML);
- на этих данных обучается модель ML для предсказания аварийности (вероятности аварии) для конкретной точки, дня недели;
- в потоке (Kafka) поступают сырые данные о текущем трафике на пунктах подсчёта (traffic);
- для этих данных подсчитываются аггрегаты (для конкретной точки, дня недели), предсказывается вероятность аварии, данные записываются в Cassandra.

![Image alt](https://github.com/Porfiryeva/Streaming/blob/main/course_project/structure.PNG)

#### 2. Подготовка датасетов

- данные о авариях
https://www.kaggle.com/datasets/daveianhickey/2000-16-traffic-flow-england-scotland-wales

- сырой трафик
https://roadtraffic.dft.gov.uk/downloads

```python
import pandas as pd

raw_traffic = pd.read_csv('dft_traffic_counts_raw_counts.csv')
accidents = pd.read_csv('accidents_2012_to_2014.csv')

# возьмём данные о трафике за 14 год для обучения модели, за 15 - для потока
raw_traffic_1415 = raw_traffic.loc[raw_traffic.Year.isin([2014, 2015])]
accidents_14 = accidents.loc[accidents.Year == 2014]

accidents_col = ['Longitude', 'Latitude', 'Day_of_Week', 'Time']
accidents_14 = accidents_14[accidents_col]

# схлопнем датасет по направлению (Direction_of_travel) и оставим только нужные столбцы
raw_traffic_1415 = raw_traffic_1415.groupby(['Count_point_id', 'Count_date', 'hour'], as_index=False).agg(
    Year=('Year', 'first'),
    Region_id=('Region_id', 'first'),
    Road_category=('Road_category', 'first'),
    Road_type=('Road_type', 'first'),
    Latitude=('Latitude', 'first'),
    Longitude=('Longitude', 'first'),
    Pedal_cycles=('Pedal_cycles', 'sum'),
    Two_wheeled_mv=('Two_wheeled_motor_vehicles', 'sum'),
    Cars_and_taxis=('Cars_and_taxis', 'sum'),
    Buses_and_coaches=('Buses_and_coaches', 'sum'),
    LGVs=('LGVs', 'sum'),
    All_HGVs=('All_HGVs', 'sum'),
    All_mv=('All_motor_vehicles', 'sum')
)

# добавим день недели (в формате вскр = 1) - данных о траффике на выходных нет
raw_traffic_1415['Day_of_Week'] = pd.to_datetime(raw_traffic_1415.Count_date).dt.weekday + 2
raw_traffic_1415['Day_of_Week'].unique()

# в accidents избавляемся от минут
accidents_14['Time'] = pd.to_datetime(accidents_14.Time).dt.hour
# и добавляем столбец is_accident
accidents_14['is_accident'] = 1

# разбиваем на данные для обучения и данные для потока
raw_traffic_14 = raw_traffic_1415.loc[raw_traffic_1415.Year == 2014]
raw_traffic_15 = raw_traffic_1415.loc[raw_traffic_1415.Year == 2015]

# сглаживаем широту и долготу чтобы получить хотя бы немного совпадений
raw_traffic_14['Latitude'] = raw_traffic_14['Latitude'] // 0.01 * 0.01 
raw_traffic_14['Longitude'] = raw_traffic_14['Longitude'] // 0.01 * 0.01 
accidents_14['Latitude'] = accidents_14['Latitude'] // 0.01 * 0.01 
accidents_14['Longitude'] = accidents_14['Longitude'] // 0.01 * 0.01 

traffic_accident = raw_traffic_14.merge(accidents_14, left_on=['Latitude', 'Longitude', 'hour', 'Day_of_Week'], right_on=['Latitude', 'Longitude', 'Time', 'Day_of_Week'], how='left')

# заполняем nan
traffic_accident.drop('Time', axis=1, inplace=True)
traffic_accident['is_accident'].fillna(0, inplace=True)

# классы плохо сбалансированы
traffic_accident.is_accident.value_counts(normalize=True)

# 0.0    0.943472
# 1.0    0.056528
# Name: is_accident, dtype: float64
    
# разбиваем неразмеченные данные (15 год) на файл для HDFS и данные для потока
roads = raw_traffic_15[['Count_point_id', 'Region_id', 'Road_category', 'Road_type', 'Latitude', 'Longitude']].drop_duplicates()
traffic = raw_traffic_15[['Count_point_id', 'Count_date', 'Day_of_Week', 'Pedal_cycles', 
                          'Two_wheeled_mv', 'Cars_and_taxis', 'Buses_and_coaches', 'LGVs', 
                          'All_HGVs', 'All_mv']]

# приводим названия столбцов к нижнему регистру
traffic_accident.columns = [col.lower() for col in traffic_accident.columns]
roads.columns = [col.lower() for col in roads.columns]
traffic.columns = [col.lower() for col in traffic.columns]

# сохранение
traffic_accident.to_parquet('traffic_for_ML', index=False)
roads.to_parquet('roads', index=False)
traffic.to_parquet('traffic', index=False)
```

- загрузка на hdfs
```bash
$ scp -P 2222 -r roads traffic traffic_for_ML raj_ops@127.0.0.1:/home/raj_ops/  raj_ops@127.0.0.1's password:
roads                                                                                             100%  174KB  19.6MB/s   00:00
traffic                                                                                           100%  804KB  38.4MB/s   00:00
traffic_for_ML                                                                                    100%  868KB  36.3MB/s   00:00

```

```bash
[raj_ops@sandbox-hdp ~]$ hdfs dfs -mkdir traffic_data
[raj_ops@sandbox-hdp ~]$ hdfs dfs -put traffic roads traffic_for_ML traffic_data/
```

#### 3. Обучение модели ML

- скрипт ML_train.py

```bash
/spark2.4.8/spark-2.4.8-bin-hadoop2.7/bin/spark-submit --driver-memory 512m --driver-cores 1 --master local[1] ML_train.py
```
    +--------------+-----------+-----------+-----------------------------------------+----------+
    |count_point_id|day_of_week|is_accident|probability                              |prediction|
    +--------------+-----------+-----------+-----------------------------------------+----------+
    |60            |2          |0.0        |[0.9446905636711379,0.055309436328862045]|0.0       |
    |60            |2          |0.0        |[0.944164381048858,0.055835618951141947] |0.0       |
    |60            |2          |0.0        |[0.9458273380452119,0.05417266195478813] |0.0       |
    |60            |2          |0.0        |[0.9475923686423331,0.05240763135766696] |0.0       |
    |60            |2          |0.0        |[0.9478146772056821,0.0521853227943179]  |0.0       |
    +--------------+-----------+-----------+-----------------------------------------+----------+
    only showing top 5 rows


#### 4. Подготовка необходимых таблиц и потоков

- создаём топик
```bash
[raj_ops@sandbox-hdp ~]$ /usr/hdp/3.0.1.0-187/kafka/bin/kafka-topics.sh --create --topic traffic --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --config retention.ms=-1
```

- записываем данные в созданный топик
```bash
[raj_ops@sandbox-hdp ~]$ export SPARK_KAFKA_VERSION=0.10
[raj_ops@sandbox-hdp ~]$ /spark2.4.8/spark-2.4.8-bin-hadoop2.7/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2
```
```python
traffic = spark.read.parquet("traffic_data/traffic")

kafka_brokers = "sandbox-hdp.hortonworks.com:6667"

traffic.selectExpr("cast(null as string) as key", "cast(to_json(struct(*)) as string) as value") \
        .write \
        .format("kafka") \
        .option("topic", "traffic") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .save()
traffic.printSchema()
```

    root
     |-- count_point_id: long (nullable = true)
     |-- count_date: string (nullable = true)
     |-- day_of_week: long (nullable = true)
     |-- pedal_cycles: long (nullable = true)
     |-- two_wheeled_mv: long (nullable = true)
     |-- cars_and_taxis: double (nullable = true)
     |-- buses_and_coaches: double (nullable = true)
     |-- lgvs: long (nullable = true)
     |-- all_hgvs: double (nullable = true)
     |-- all_mv: double (nullable = true)

- создаём таблицу в Cassandra
```bash
[raj_ops@sandbox-hdp ~] sudo service cassandra start
[raj_ops@sandbox-hdp ~] cqlsh
```
```SQL
CREATE KEYSPACE IF NOT EXISTS traffic_ml WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE traffic_ML;
CREATE TABLE traffic (
                      count_point_id int,
                      region_id int,
                      road_category text,
                      road_type text,
                      latitude float,
                      longitude float,
                      pedal_cycles int,
                      two_wheeled_mv int,
                      cars_and_taxis float,
                      buses_and_coaches float,
                      lgvs int,
                      all_hgvs float,
                      all_mv float,
                      day_of_week int,
                      p_accident float,
                      primary key (count_point_id, day_of_week)
                      );
```

- заполним часть данных (характеристики участка дороги) в таблице + приджойненные дни недели для каждого пункта
```python
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType
from pyspark.sql import functions as F

roads = spark.read.parquet('traffic_data/roads')
day_of_week = spark.createDataFrame([(i,) for i in range(1, 8)], "day_of_week int")
tmp = roads.crossJoin(day_of_week)

tmp.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="traffic", keyspace="traffic_ml") \
    .mode("append") \
    .save()
```

- для проверки
```SQL
cqlsh:traffic_ml> SELECT count_point_id, day_of_week, road_type, p_accident FROM traffic LIMIT 5;

 count_point_id | day_of_week | road_type | p_accident
----------------+-------------+-----------+------------
         948827 |           1 |     Minor |       null
         948827 |           2 |     Minor |       null
         948827 |           3 |     Minor |       null
         948827 |           4 |     Minor |       null
         948827 |           5 |     Minor |       null
```

#### 5. Запуск приложения

- скрипт ML_stream.py
```bash
/spark2.4.8/spark-2.4.8-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1] ML_stream.py
```
- вывод скрипта

--

    ---------I've got new batch--------
    This is what I've got from Kafka:
    +--------------+-----------+------------+--------------+--------------+-----------------+----+--------+------+------+
    |count_point_id|day_of_week|pedal_cycles|two_wheeled_mv|cars_and_taxis|buses_and_coaches|lgvs|all_hgvs|all_mv|offset|
    +--------------+-----------+------------+--------------+--------------+-----------------+----+--------+------+------+
    |           505|          6|           5|             1|         247.0|              9.0|  79|     3.0| 339.0|    12|
    |           505|          6|          11|             2|         976.0|             22.0|  89|    10.0|1099.0|    13|
    |           505|          6|           9|             4|         750.0|             12.0|  94|     7.0| 867.0|    14|
    |           505|          6|           4|             7|         611.0|             11.0|  74|     6.0| 709.0|    15|
    +--------------+-----------+------------+--------------+--------------+-----------------+----+--------+------+------+

    Here is the sums from Kafka:
    +--------------+-----------+---------+---------+---------+-------------+---------+--------+------+-----------------+--------------+----+------------+--------------+
    |count_point_id|day_of_week| latitude|longitude|region_id|road_category|road_type|all_hgvs|all_mv|buses_and_coaches|cars_and_taxis|lgvs|pedal_cycles|two_wheeled_mv|
    +--------------+-----------+---------+---------+---------+-------------+---------+--------+------+-----------------+--------------+----+------------+--------------+
    |           505|          6|-999999.0|-999999.0|        0|             |         |    10.0|1099.0|             22.0|         976.0|  94|          11|             7|
    +--------------+-----------+---------+---------+---------+-------------+---------+--------+------+-----------------+--------------+----+------------+--------------+

    I'm gonna select this from Cassandra:
     (count_point_id = 505 and day_of_week = 6)
    Here is what I've got from Cassandra:
    +--------------+-----------+--------+---------+---------+-------------+---------+--------+------+-----------------+--------------+----+------------+--------------+
    |count_point_id|day_of_week|latitude|longitude|region_id|road_category|road_type|all_hgvs|all_mv|buses_and_coaches|cars_and_taxis|lgvs|pedal_cycles|two_wheeled_mv|
    +--------------+-----------+--------+---------+---------+-------------+---------+--------+------+-----------------+--------------+----+------------+--------------+
    |           505|          6|53.22008| -4.15824|        4|           PA|    Major|     0.0|   0.0|              0.0|           0.0|   0|           0|             0|
    +--------------+-----------+--------+---------+---------+-------------+---------+--------+------+-----------------+--------------+----+------------+--------------+

    Here is how I aggregated Cassandra and Kafka:
    +--------------+-----------+----------------+------------------+---------+-------------+---------+--------+------+-----------------+--------------+----+------------+--------------+
    |count_point_id|day_of_week|        latitude|         longitude|region_id|road_category|road_type|all_hgvs|all_mv|buses_and_coaches|cars_and_taxis|lgvs|pedal_cycles|two_wheeled_mv|
    +--------------+-----------+----------------+------------------+---------+-------------+---------+--------+------+-----------------+--------------+----+------------+--------------+
    |           505|          6|53.2200813293457|-4.158239841461182|        4|           PA|    Major|    10.0|1099.0|             22.0|         976.0|  94|          11|             7|
    +--------------+-----------+----------------+------------------+---------+-------------+---------+--------+------+-----------------+--------------+----+------------+--------------+

    It will be written into Cassandra
    +--------------+-----------+----------------+------------------+---------+-------------+---------+--------+------+-----------------+--------------+----+------------+--------------+----------+
    |count_point_id|day_of_week|        latitude|         longitude|region_id|road_category|road_type|all_hgvs|all_mv|buses_and_coaches|cars_and_taxis|lgvs|pedal_cycles|two_wheeled_mv|p_accident|
    +--------------+-----------+----------------+------------------+---------+-------------+---------+--------+------+-----------------+--------------+----+------------+--------------+----------+
    |           505|          6|53.2200813293457|-4.158239841461182|        4|           PA|    Major|    10.0|1099.0|             22.0|         976.0|  94|          11|             7|0.04834963|
    +--------------+-----------+----------------+------------------+---------+-------------+---------+--------+------+-----------------+--------------+----+------------+--------------+----------+

- запись в Cassandra
```SQL
SELECT * FROM traffic WHERE p_accident >= 0 ALLOW FILTERING;
```
         count_point_id | day_of_week | all_hgvs | all_mv | buses_and_coaches | cars_and_taxis | latitude | lgvs | longitude | p_accident | pedal_cycles | region_id | road_category | road_type | two_wheeled_mv
    ----------------+-------------+----------+--------+-------------------+----------------+----------+------+-----------+------------+--------------+-----------+---------------+-----------+----------------
                501 |           3 |      796 |  10092 |                53 |           8552 |   -1e+06 | 1385 |    -1e+06 |          0 |            0 |         0 |               |           |             34
                505 |           6 |       10 |   1099 |                22 |            976 | 53.22008 |   94 |  -4.15824 |    0.04835 |           11 |         4 |            PA |     Major |              7
             942551 |           4 |       11 |    714 |                18 |            584 | 51.40791 |   93 | -0.105593 |   0.099674 |            2 |         6 |            MB |     Minor |              8
             942560 |           2 |        1 |    146 |                11 |            115 | 51.37079 |   18 | -0.042917 |   0.110273 |            0 |         6 |           MCU |     Minor |              1
             942569 |           3 |        0 |     10 |                 0 |             10 | 51.40246 |    0 | -0.097939 |   0.108896 |            0 |         6 |           MCU |     Minor |              0
             942566 |           4 |        2 |    191 |                 4 |            172 |  51.3236 |   12 | -0.098893 |   0.115613 |            3 |         6 |           MCU |     Minor |              1

    (6 rows)

- как видно, скрипт отработал, данные поступают в потоке, агрегаты обновляются, модель предсказывает вероятность аварии для конкретного дня недели и наблюдательного пункта.
