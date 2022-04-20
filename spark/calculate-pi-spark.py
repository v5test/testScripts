import os
from time import time
from random import random
from operator import add
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('CalculatePi').getOrCreate()
sc = spark.sparkContext

slices = int(os.environ['IP_ADVANCED_NUMBEROFEXECUTORS'])

n = 100000 * slices


def is_point_inside_unit_circle(p):
    # p is useless here
    x, y = random(), random()
    return 1 if x * x + y * y < 1 else 0


t_0 = time()

# parallelize creates a spark Resilient Distributed Dataset (RDD)
# its values are useless in this case
# but allows us to distribute our calculation (inside function)
count = sc.parallelize(range(0, n), slices) \
    .map(is_point_inside_unit_circle).reduce(add)
print("%.6f seconds elapsed for spark approach and n=%d" % (time() - t_0, n))
print("Pi is roughly %.6f" % (4.0 * count / n))

# VERY important to stop SparkSession
# Otherwise, the job will keep running indefinitely
spark.stop()