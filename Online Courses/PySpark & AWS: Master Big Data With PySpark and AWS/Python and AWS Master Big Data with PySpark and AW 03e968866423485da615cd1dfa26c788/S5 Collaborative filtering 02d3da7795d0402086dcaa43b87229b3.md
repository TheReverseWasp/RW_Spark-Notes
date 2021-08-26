# S5. Collaborative filtering

# Utility Matrix

![Untitled](S5%20Collaborative%20filtering%2002d3da7795d0402086dcaa43b87229b3/Untitled.png)

# Explicit and Implicit Ratings

- Explicit: Ratings itself.
- Implicit: Other information, such as hours expended, access time, etc.

Note: Is easier to work with Explicit Ratings.

# Expected Results

![Untitled](S5%20Collaborative%20filtering%2002d3da7795d0402086dcaa43b87229b3/Untitled%201.png)

# Hands on

## Dataset Overview

- Movie dataset
- 2 Files
    - Movie names
    - Ratings by users

```python
dbutils.fs.rm("PATH", True) # removes all files in PATH
# maybe cause some problems in databricks but you can solution it

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Collaborative filtering").getOrCreate()

moviesDF = spark.read.options(header="True", inferSchema="True").csv("PATH/movies.csv")
ratingsDF = park.read.options(header="True", inferSchema="True").csv("PATH/ratings.csv")

movieDF.printSchema()
ratingsDF.printSchema()
```

## Joining DFs

```python
display(moviesDF) #better view of dataframe
display(ratingsDF)

ratingsDF.join(moviesDF, "movieID", "left").show()
```

## Create Train and Test DataFrames

```python
ratings = ratingsDF.join(moviesDF, "movieID", "left").show()

(train, test) = ratings.randomSplit([0.8, 0.2])
```

## Create a model

```python
from pyspark.ml.recommendation import ALS
als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", nonnegative=True, implicitPrefs=False, coldStartStrategy="drop")
```

## Hyperparameter tuning and Cross validation

```python
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

param_grid = ParamGridBuilder() \
						.addGrid(als.rank, [10, 50, 100, 150]) \
						.addGrid(als.regParam, [.01, .05, .1, .15]) \
						.build()

evaluator = RegressionEvaluator(
						metricName="rmse",
						labelCol="rating",
						predictionCol="prediction")

cv = CrossValidator(estimator=als, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)
```

## Training and Testing

```python
model = cv.fit(train)
bestModel = model.bestModel

testPredictions = bestModel.transform(test)
RMSE = evaluator(testPredictions)
print(RMSE)
```

## Recommendations

```python
df = best_model.recommendForAllUsers(5) # Top 5 recommended movies for each user

df2 = df.withColumn("movieid_rating", explode("recommendations")) #

display(df2.select("userId", col("movieid_rating.movieId"), col("movieid_rating.rating")))
```