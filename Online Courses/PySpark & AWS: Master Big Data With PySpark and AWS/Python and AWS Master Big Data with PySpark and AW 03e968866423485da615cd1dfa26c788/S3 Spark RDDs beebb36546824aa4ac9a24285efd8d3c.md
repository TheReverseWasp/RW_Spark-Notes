# S3: Spark RDDs

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled.png)

# Transformations and Actions

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%201.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%201.png)

# Creating Spark RDD (Online)

- Dentro de Databricks community
    - Crear un Notebook en un cluster
    - Ir a la sección Data
        - Crear una tabla y subir el archivo
    - En el notebook escribir

```python
#1
# Imports comunes
from pyspark import SparkConf, SparkContext

#2
#Agregar la configuración
conf = SparkConf().setAppName("Read File")

#3
# Agregar el contexto
sc = SparkContext.getOrCreate(conf=conf)

#4 
## Ir al archivo, hacerle click y ver su path
text = sc.textFile("PATH/to/file.txt")

#5 
#print metadata
text
#processing creates RDDs for each line
text.collect()
```

# Running Code Locally

1. Exportar el notebook como source file
2. Modificar la linea:

```python
#4
#path del archivo localmente
text= sc.textFile("PATH/for/local/file.txt")

#5
###...
print(text)
###...
print(text.collect())
```

1. Correr la linea

```
spark-submit "name of the file.py"
```

1. Si ocurre una excepción es por que tienes diferentes versiones de python en tu PC.
    - Usualmente pyspark espera python3 pero no tiene python3

```
## En windows
set PYSPARK_PYTHON=python
```

# RDD Functions

## Map (Lambda)

```python
#1
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Read File")
sc = SparkContext.getOrCreate(conf=conf)

#2
rdd = sc.textFile("PATH/in/DB/file.txt")

#3 
rdd2 = rdd.map(lambda x: x.split(' '))

#4
rdd2.collect()
```

## Map (Function)

```python
#1. Same as before
def fun(x):
	return x.split(' ')

rdd2 = rdd.map(fun)

#2 
def foo(x):
	l = x.split()
	l2 = []
	for s in l:
		l2.append(int(s) + 2)
	return l2

rdd3 = rdd.map(foo)

rdd2.collect()
rdd3.collect()
```

## Quiz

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%202.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%202.png)

### Solution 1

```python
def quiz(x):
	l = x.split(' ')
	l2 = []
	for s in l:
		l2.append(len(x))
	return l2

rdd2 = rdd.map(quiz)	
rdd.collect()
```

### Solution 2

```python
rdd3 = rdd.map(x: [len(s) for s in x.split(' ')])
rdd3.collect()
```

## flatMap()

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%203.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%203.png)

```python
flatMapRdd = rdd.flatMap(x: x.split(" ")
flatMapRdd.collect()
```

## filter()

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%204.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%204.png)

```python
rdd2 = rdd.filter(lambda x: x != "Something")
rdd2.collect()
```

## Quiz

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%205.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%205.png)

### Solution 1

```python
#1 No recomendado

rdd2 = rdd.flatMap(lambda x: x.split(' ')).filter(lambda x: x[0] != 'a' and x[0] != 'c')
rdd2.collect()

#2
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.filter(lambda x: x[0] != 'a' and x[0] != 'c')
rdd3.collect()
```

### Solution 2

```python
def foo(x):
	l = x.split(' ')
	for s in l:
	# if s.startswith('a') or s.startswith('c')
		if s[0] != 'a' and s[0] != 'c':
			return True
		else:
			return False

rdd4 = rdd.flatMap(foo)
rdd4.collect()
```

### Solution N

Permutations

- Usar funciones para condiciones largas

Notas: Es un codigo tortuga, cuando se hace collect se hace todo lo de atras hasta el anterior RDD.

## distinct()

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%206.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%206.png)

```python
#1
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd.distinct()

#2
rdd4 = rdd.flatMap(lambda x: x.split(' ')).distinct() #.collect()
rdd4.collect()

```

## groupByKey()

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%207.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%207.png)

```python
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x,1))
rdd3.groupByKey().collect() # iterables
rdd3.groupByKey().mapValues(list).collect() #list
```

## reduceByKey()

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%208.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%208.png)

```python
rdd2 = rdd.flaptMap(lambda x: x.split(' ')
rdd3 = rdd2.map(lambda x: (x,1))
rdd3.reduceByKey(lambda x, y: x + y).collect()
```

## Quiz

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%209.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%209.png)

```python
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x, 1))
rdd3.reduceByKey(lambda x,y: x + y).collect()
```

## count()

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2010.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2010.png)

```python
rdd.flatMap(lambda x: x.split(' ').count()
```

## countByValue()

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2011.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2011.png)

```python
rdd.flatMap(lambda x: x.split(' ').countByValue()
```

## saveAsTextFile()

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2012.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2012.png)

```python
#1
rdd.saveAsTextFile('PATH/MUST/EXIST/output/filename.txt)

#2 
rdd.saveAsTextFile()

#always creates a folder
```

## repartition()

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2013.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2013.png)

## coalesce()

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2014.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2014.png)

```python
rdd.getNumPartitions() #2

rdd2 = rdd.repartition(5)

rdd2.getNumPartitions() #5
```

```python
rdd3 = rdd.coleasce(2)

rdd3.getNumPartitions() #2
```

Nota: si se selecciona un directorio en sc.textFile() se lee todos los archivos.

# Finding Average

```python
rdd = sc.textFile("PATH") #movie, score

rdd2 = rdd.map(lambda x: (x.split(",")[0], int(x.split(",")[1])))

rdd3 = rdd2.map(lambda x: (x[0],(x[1], 1)))

rdd4 = rdd3.reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))

rdd5 = rdd4.map(lambda x: (x[0], x[1][0] / x[1][1]))
rdd5.collect()

rdd5.saveAsTextFile("PATH2")
```

## Quiz

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2015.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2015.png)

```python
rdd = sc.textFile("PATH") #MM,city,score

rdd2 = rdd.map(lambda x: (x.split(",")[0], (float(x.split(",")[2]), 1))

rdd3 = rdd2.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))

rdd4 = rdd3.map(lambda x: (x[0], x[1][0]/x[1][1]))
```

# Finding Min and Max

```python
rdd = sc.textFile("PATH") #movie, score

rdd2 = rdd.map(lambda x: (x.split(",")[0], int(x.split(",")[1])))

rddmin = rdd2.reduceByKey(lambda x,y: x if x < y else y)

rddmax = rdd2.reduceByKey(lambda x,y: y if x < y else x)
```

## Quiz

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2016.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2016.png)

```python
rdd = sc.textFile("PATH") #MM,city,score

rdd2 = rdd.map(lambda x: (x.split(',')[1], float(x.split(',')[2])))

rddmin = rdd2.reduceByKey(lambda x,y: x if x < y else y)

rddmax = rdd2.reduceByKey(lambda x,y: x if x > y else y)
```

# Mini Project

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2017.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2017.png)

![S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2018.png](S3%20Spark%20RDDs%20beebb36546824aa4ac9a24285efd8d3c/Untitled%2018.png)

```python
rdd = sc.TextFile("PATH")
rdd.collect()
#tiene la primera fila con el nombre de las columnas

headers = rdd.first() #obtiene la primera fila

rdd = rdd.filter(lambda x: x!= headers)
```

1.

```python
rdd.count()
```

2.

```python
rdd = rdd.map(lambda x: x.split(','))
rdd2 = rdd
```

```python
rdd2 = rdd2.map(lambda x: (x[1], int(x[5])))
rdd2 = rdd2.reduceByKey(lambda x,y : x+y)
rdd.collect()
```

3.

```python
rdd3 = rdd
passed = rdd3.filter(lambda x: int(x[5]) > 50).count()
failed = rdd3.filter(lambda x: int(x[5]) <= 50).count()

print(passed, failed)
#failed = rdd.count() - passed
```

4.

```python
rdd4 = rdd
rdd4 = rdd4.map(lambda x: (x[3],1))
rdd4 = rdd.reduceByKey(lambda x.y: x+y)
rdd4.collect()
```

5.

```python
rdd5 = rdd
rdd5 = rdd5.map(lambda x: (x[3], int(x[5])))
rdd5 = rdd5.reduceByKey(lambda x,y: x+y)
rdd5.collect()
```

6.

```python
rdd6 = rdd
rdd6 = rdd6.map(lambda x: (x[3], int(x[5],1)))
rdd6 = rdd6.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
rdd6 = rdd6.map(lambda x: (x[0], x[1][0]/x[1][10]))
rdd6.collect()
```

### mapValues()

```python
rdd6.mapValues(lambda x: (x[0]/x[1])).collect()
#the same as the previous action before collect()
```

7.

```python
rdd7 = rdd
rdd7 = rdd7.map(lambda x: (x[3], int(x[5])))
#min
minimum = rdd7.reduceByKey(lambda x,y: x if x < y else y)
#max
maximum = rdd7.reduceByKey(lambda x,y: x if x > y else y)
print(minumum)
print(maximum)
```

8.

```python
rdd8 = rdd
rdd8 = rdd8.map(lambda x: (x[1],(int(x[0]),1)))
rdd8 = rdd8.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
rdd8 = rdd8.mapValues(lambda x: x[0]/x[1])
rdd8.collect()
```