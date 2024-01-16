from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
import joblib
import numpy
import socket

spark = SparkSession.builder.appName("stream2").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 5)

loaded_model = joblib.load('model.joblib')

lines = ssc.socketTextStream("localhost", 9999).flatMap(lambda x: x.split("\n"))
lines.pprint()
patient=lines.map(lambda line:  list(map(float,line.split(','))))
patient.pprint()
def process(input):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("namenode",9999))
    s.send('At risk'.encode())
    s.close()
    if loaded_model.predict(input) == 1:
        return "At risk"
    else:
        return "No risk"

patient.map( lambda liste : process(numpy.array(liste).reshape(1, -1))).pprint()

ssc.start()
ssc.awaitTermination()
