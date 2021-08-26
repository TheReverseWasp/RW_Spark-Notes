# S6: Spark Streaming

![Untitled](S6%20Spark%20Streaming%20a06c0a9b0d554fbb91f66ce9849f7d2c/Untitled.png)

![Untitled](S6%20Spark%20Streaming%20a06c0a9b0d554fbb91f66ce9849f7d2c/Untitled%201.png)

# Spark Streaming With RDD

```python
from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Streaming")
sc = SparkContext.getOrCreate(conf=conf)

ssc= StreamingContext(sc, 1)
```

```python
rdd = ssc.textFileStream("PATH/to/all_files/")
```

```python
rdd.pprint()

ssc.start()
ssc.awaitTerminationOrTimeout(100)
```

Note: If you are experiment errors in Databricks you just need to restart the notebook/cluster.

```python
ssc.stop()
```

## Transformations

```python
rdd = rdd.map(lambda x: (x,1))
rdd.reduceByKey(lambda x,y: x+y)

rdd.pprint()
ssc.start()
ssc.awaitTerminationOrTimeout(100)
```

# Spark Streaming in DF

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark Streaming DF").getOrCreate()
word = spark.readStream.text("PATH/to/files_directory")
word.writeStream.format("console").outputMode("complete").start()
```

## Spark Streaming Display

```python
#1 Dispkays the data in Table Format
display(word)
```

## Aggregations

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark Streaming DF").getOrCreate()
word = spark.readStream.text("PATH/to/files_directory")
word.writeStream.format("console").outputMode("complete").start()
word = word.groupBy("value").count()

```

```python
display(word)
```