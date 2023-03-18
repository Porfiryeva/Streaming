## Урок 6. Spark Streaming. Cassandra.

##### Задание 1. Поработать с Cassandra через консоль. Протестировать инсерты, селекты с разными ключами. Работать в keyspace lesson7. Там можно создать свои таблички.


- подключение
```bash
ssh ssh raj_ops@127.0.0.1 -p 2222
```

- для старта Cassandra
`sudo service cassandra start`

```bash
[raj_ops@sandbox-hdp ~]$ sudo service cassandra start
Starting cassandra (via systemctl):                        [  OK  ]
[raj_ops@sandbox-hdp ~]$ cqlsh
```

Далее все команды в терминале кассандры. 

- все таблицы указанного keyspace
```SQL
cqlsh> SELECT keyspace_name, table_name FROM system_schema.tables WHERE keyspace_name = 'test';

 keyspace_name | table_name
---------------+------------
          test |         kv

(1 rows)

```

- создаём keyspace
```SQL
cqlsh> CREATE KEYSPACE IF NOT EXISTS lesson7 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
cqlsh> use lesson7;
```

Создаём новую табличку:
- primary key м.б. составной
```SQL
cqlsh:lesson7> CREATE TABLE animals (
           ... id int,
           ... name text,
           ... size text,
           ... primary key (id)
           ... );
```

Вставка записи:
```SQL
cqlsh:lesson7> INSERT INTO animals (id, name, size)
           ... VALUES (3, 'Deer', 'Big');
cqlsh:lesson7> SELECT * FROM animals;

 id | name | size
----+------+------
  3 | Deer |  Big

(1 rows)
```

Апдейт записи с `id = 3`: - осуществляем INSERT по существующему ключу

```bash
cqlsh:lesson7> INSERT INTO animals (id, name) VALUES (3, 'Bear');
cqlsh:lesson7> SELECT * FROM animals;

 id | name | size
----+------+------
  3 | Bear |  Big

(1 rows)

```

Вставка ещё одной записи:

```bash
cqlsh:lesson7> INSERT INTO animals (id, name) VALUES (5, 'Snake');
cqlsh:lesson7> SELECT * FROM animals;

 id | name  | size
----+-------+------
  5 | Snake | null
  3 |  Bear |  Big

(2 rows)
```

Удаление по ключу не отработает. Это особенность консольной утилиты.

```SQL
cqlsh:lesson7> DELETE id FROM animals WHERE id = 3;
InvalidRequest: Error from server: code=2200 [Invalid query] message="Invalid identifier id for deletion (should not be a PRIMARY KEY part)"
```

Удалить запись можно, затерев старые значения.

```SQL
cqlsh:lesson7> INSERT INTO animals (id, name, size) VALUES (3, null, null);
cqlsh:lesson7> SELECT * FROM animals;

 id | name  | size
----+-------+------
  5 | Snake | null
  3 |  null | null

(2 rows)
```

ещё несколько записей
```SQL
cqlsh:lesson7> INSERT INTO animals (id, name, size) VALUES (2, 'Cat', 'Small');
cqlsh:lesson7> INSERT INTO animals (id, name, size) VALUES (1, 'Dog', 'Medium');
cqlsh:lesson7> INSERT INTO animals (id, name, size) VALUES (4, 'Bear', 'Big');
cqlsh:lesson7> SELECT * FROM animals ;

 id | name  | size
----+-------+--------
  5 | Snake |   null
  1 |   Dog | Medium
  2 |   Cat |  Small
  4 |  Bear |    Big
  3 |  Deer |    Big

(5 rows)
cqlsh:lesson7> exit
```

- чтобы удалить таблицу

```SQL
cqlsh:lesson7> DROP TABLE animals ;
cqlsh:lesson7> SELECT * FROM animals;
InvalidRequest: Error from server: code=2200 [Invalid query] message="unconfigured table animals"
```

- аггрегационные функции по большим данным могут не выполняться 


###### HBASE

Тут повторим все те же операции для другой базы. 

- предварительно start HBase
Запускаем консольный клиент:

```bash
hbase shell
```

Создаём новое namespace и табличку:

```bash
hbase(main):002:0> create_namespace 'lesson7'
Took 0.3647 seconds
hbase(main):003:0> create 'lesson7:animals', 'name', 'size'
Created table lesson7:animals
Took 2.4039 seconds
=> Hbase::Table - lesson7:animals
```

Вставка записи:

```bash
hbase(main):004:0> put 'lesson7:animals', '3', 'name', 'Deer'
Took 0.2262 seconds
hbase(main):005:0> put 'lesson7:animals', '3', 'size', 'Big'
Took 0.0254 seconds
hbase(main):007:0> scan 'lesson7:animals'
ROW                                COLUMN+CELL
 3                                 column=name:, timestamp=1679126994083, value=Deer
 3                                 column=size:, timestamp=1679127004027, value=Big
1 row(s)
Took 0.0231 seconds

```

Апдейт записи с `id = 3`:

```bash
hbase(main):008:0> put 'lesson7:animals', '3', 'name', 'Doe'
Took 0.0066 seconds
hbase(main):009:0> scan 'lesson7:animals'
ROW                                COLUMN+CELL
 3                                 column=name:, timestamp=1679127117370, value=Doe
 3                                 column=size:, timestamp=1679127004027, value=Big
1 row(s)
Took 0.0152 seconds
```

Вставка ещё одной записи:

```bash
hbase(main):010:0> put 'lesson7:animals', '5', 'name', 'Snake'
Took 0.0233 seconds
hbase(main):011:0> scan 'lesson7:animals'
ROW                                COLUMN+CELL
 3                                 column=name:, timestamp=1679127117370, value=Doe
 3                                 column=size:, timestamp=1679127004027, value=Big
 5                                 column=name:, timestamp=1679127177610, value=Snake
2 row(s)
Took 0.0262 seconds
```

Удаление всех колонок по ключу:

```bash
hbase(main):012:0> deleteall 'lesson7:animals', '3'
Took 0.0559 seconds
hbase(main):013:0> scan 'lesson7:animals'
ROW                                COLUMN+CELL
 5                                 column=name:, timestamp=1679127177610, value=Snake
1 row(s)
Took 0.0158 seconds
```

В конце удалим табличку:

```bash
hbase(main):014:0> disable 'lesson7:animals'
Took 1.3933 seconds
hbase(main):015:0> drop 'lesson7:animals'
Took 0.5095 seconds
hbase(main):016:0> scan 'lesson7:animals'
ROW                                COLUMN+CELL

ERROR: Unknown table lesson7:animals!
hbase(main):017:0> exit
``` 

##### Задание 2. Когда cassandra станет понятна, поработать с ней через Spark.

Запускаем pyspark с указанием библиотеки для работы с cassandra.
```bash
[raj_ops@sandbox-hdp ~]$ export SPARK_KAFKA_VERSION=0.10
[raj_ops@sandbox-hdp ~]$ /spark2.4.8/spark-2.4.8-bin-hadoop2.7/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2
```

Делаем стандартные импорты и читаем табличку `lesson7.animals`.
```python
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F

cass_animals_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="animals", keyspace="lesson7") \
    .load()

cass_animals_df.printSchema()
```

    root
     |-- id: integer (nullable = true)
     |-- name: string (nullable = true)
     |-- size: string (nullable = true)


Посмотрим, что есть в таблице: 

```python
cass_animals_df.show()
```

    +---+-----+------+
    | id| name|  size|
    +---+-----+------+
    |  5|Snake|  null|
    |  1|  Dog|Medium|
    |  2|  Cat| Small|
    |  4| Bear|   Big|
    |  3| Deer|   Big|
    +---+-----+------+


Создадим запись с ключом 11 и добавим её в таблицу.
```python
cow_df = spark.sql("""select 11 as id, "Cow" as name, "Big" as size """)
cow_df.show()
```

    +---+----+----+
    | id|name|size|
    +---+----+----+
    | 11| Cow| Big|
    +---+----+----+

Добавляем с указанием режима `append`.

```python
cow_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="animals", keyspace="lesson7") \
    .mode("append") \
    .save()

cass_animals_df.show()
```

    +---+-----+------+
    | id| name|  size|
    +---+-----+------+
    |  5|Snake|  null|
    | 11|  Cow|   Big|
    |  1|  Dog|Medium|
    |  2|  Cat| Small|
    |  4| Bear|   Big|
    |  3| Deer|   Big|
    +---+-----+------+

    
Не смотря на то что при записи указывался режим `append`, фактически будет произведён `update` записи, если такой ключ уже существовал в таблице.


```python
cass_animals_df.filter(F.col("id")=="11").show()
```

    +---+----+----+
    | id|name|size|
    +---+----+----+
    | 11| Cow| Big|
    +---+----+----+


##### Задание 3. Проверить пушит ли спарк фильтры в касандру.

Метод `explain`, который показывает логический и физический план запроса.
```python
def explain(self, extended=True):
    if extended:
        print(self._jdf.queryExecution().toString())
    else:
        print(self._jdf.queryExecution().simpleString())
```

Проверим что находится в PushedFilters в физическом плане при разных запросах.

Запрос 1:
```python
cass_animals_df.filter(F.col("id")=="11").explain()
```

    == Physical Plan ==
    *(1) Filter isnotnull(id#0)
    +- *(1) Scan org.apache.spark.sql.cassandra.CassandraSourceRelation@1f71aff9 [id#0,name#1,size#2] PushedFilters: [IsNotNull(id), *EqualTo(id,11)], ReadSchema: struct<id:int,name:string,size:string>

- фильтрация выполняется не на стороне приложения, запрос оптимальный

Запрос 2:
```python
cass_animals_df.filter(F.col("name")=="Cat").explain()
```

    == Physical Plan ==
    *(1) Filter (isnotnull(name#1) && (name#1 = Cat))
    +- *(1) Scan org.apache.spark.sql.cassandra.CassandraSourceRelation@1f71aff9 [id#0,name#1,size#2] PushedFilters: [IsNotNull(name), EqualTo(name,Cat)], ReadSchema: struct<id:int,name:string,size:string>


Оба запроса спускают фильтр до уровня базы. Фильтр по ключу сделается быстро, первый запрос оптимальный.

Сделаем представление `cass_df` датафрейма `cass_big_df`, чтобы обращаться к нему внутри SQL-выражений. 

```python
cass_animals_df.createOrReplaceTempView("animals")
spark.table("animals").show()
```

    23/03/18 08:56:06 WARN shortcircuit.DomainSocketFactory: The short-circuit local reads feature cannot be used because libhadoop cannot be loaded.
    23/03/18 08:56:06 WARN metastore.ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
    +---+-----+------+
    | id| name|  size|
    +---+-----+------+
    |  5|Snake|  null|
    | 11|  Cow|   Big|
    |  1|  Dog|Medium|
    |  2|  Cat| Small|
    |  4| Bear|   Big|
    |  3| Deer|   Big|
    +---+-----+------+


```python
spark.table("animals").explain()
```
    == Physical Plan ==
    *(1) Scan org.apache.spark.sql.cassandra.CassandraSourceRelation@1f71aff9 [id#0,name#1,size#2] PushedFilters: [], ReadSchema: struct<id:int,name:string,size:string>


Запрос 3:

- `between 1 and 4`
```python
spark.sql("select * from animals where id between 1 and 4").explain()
```

    == Physical Plan ==
    *(1) Filter ((isnotnull(id#0) && (id#0 >= 1)) && (id#0 <= 4))
    +- *(1) Scan org.apache.spark.sql.cassandra.CassandraSourceRelation@1f71aff9 [id#0,name#1,size#2] PushedFilters: [IsNotNull(id), GreaterThanOrEqual(id,1), LessThanOrEqual(id,4)], ReadSchema: struct<id:int,name:string,size:string>

- запрос оптимальный

```python
spark.sql("select * from animals where id in(1, 4)").show()
```
    +---+----+------+
    | id|name|  size|
    +---+----+------+
    |  1| Dog|Medium|
    |  4|Bear|   Big|
    +---+----+------+


Запрос 4:

```python
spark.sql("select * from animals where id in(1, 4)").explain()
```

    == Physical Plan ==
    *(1) Scan org.apache.spark.sql.cassandra.CassandraSourceRelation@1f71aff9 [id#0,name#1,size#2] PushedFilters: [*In(id, [1,4])], ReadSchema: struct<id:int,name:string,size:string>


В PushedFilters есть фильтр по ключам. Запрос оптимальный.
