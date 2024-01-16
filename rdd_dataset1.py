from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import when

def delete_columns(line):
    columns = line.split(',')
    columns_to_remove = [0, 5, 22, 23, 24]
    new_columns = [columns[i] for i in range(len(columns)) if i not in columns_to_remove]
    return ','.join(new_columns)


conf = SparkConf().setAppName("RDD_HeartAttack")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

rdd = sc.textFile('hdfs://<YOUR IP>:9000/input_heart_attack/heart_attack_dataset1.txt')
transformed_rdd = rdd.map(lambda line: line.replace(",Male,", ",0,").replace(",Female,", ",1,"))
transformed_rdd = transformed_rdd.map(lambda line: line.replace(",Healthy,", ",2,").replace(",Unhealthy,", ",0,").replace(",Average,", ",1,"))

rdd_filtred = transformed_rdd.map(delete_columns)
rdd_filtred.saveAsTextFile('hdfs://<YOUR IP>:9000/output_heart_attack/new_dataset1.csv')
sc.stop()
