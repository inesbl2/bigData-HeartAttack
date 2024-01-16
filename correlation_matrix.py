from pyspark.sql import SparkSession
from pyspark.ml.stat import Correlation as corr
import pyspark.sql.functions as F
from pyspark.sql.functions import col


spark = SparkSession.builder \
    .appName("Correlation Matrix") \
    .getOrCreate()

df1 = spark.read.format("csv").option("header", "True").load('hdfs://<YOUR IP>:9000/output_heart_attack/new_dataset1.csv/part-00000')
df2 = spark.read.format("csv").option("header", "False").load('hdfs://<YOUR IP>:9000/output_heart_attack/new_dataset1.csv/part-00001')
df = df1.union(df2)
df = df.drop('Blood Pressure')
df = df.select(
    *(F.col(c).cast('float').alias(c) for c in df.columns))


dff = spark.read.format("csv").option("header", "True").load('hdfs://<YOUR IP>:9000/input_heart_attack/heart_attack_dataset2.csv')
dff = dff.select(
    *(F.col(c).cast('float').alias(c) for c in dff.columns))



def is_intresting (df, target, dataset_name):
    liste=[]
    liste2=[]
    for column in df.columns :
        liste.append(abs(df.corr(column, target)))
    
    coeff = 0
    for element in liste : 
        coeff += element 
    coeff = coeff / len(liste)
    
    df = df.toPandas()
    matrix = df.corr()
    print(matrix)

    if coeff < 0.1 :
        print("with a coeff of", coeff, "-------------------------------------- This dataset", dataset_name ,"doesn't seem to be intresting for a prediction")
    else :
        print("with a coeff of", coeff,"-------------------------------------- This dataset", dataset_name ,"seems to be intresting for a prediction")



is_intresting(df, 'Heart Attack Risk', 'heart_attack_dataset1.txt')
is_intresting(dff, 'target', 'heart_attack_dataset2.csv')



