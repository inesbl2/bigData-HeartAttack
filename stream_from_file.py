from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
import joblib
import numpy

# Create a local StreamingContext with two working thread and batch interval of 5 seconds
spark = SparkSession.builder.appName("stream2").getOrCreate()

sc = spark.sparkContext
ssc = StreamingContext(sc, 5)

# Load the saved model
loaded_model = joblib.load('model.joblib')

# Create a DStream of lines from the HDFS directory
lines = ssc.textFileStream("hdfs://<YOUR IP>:9000/stream").flatMap(lambda x: x.split("\n"))
lines.pprint()
patient=lines.map(lambda line:  list(map(float,line.split(','))))
patient.pprint()

t_list = patient.map( lambda res : ('patient =>',res,'has a risk attack of',loaded_model.predict(numpy.array(res).reshape(1, -1))))
t_list.pprint(20)

 
ssc.start()
ssc.awaitTermination()
