from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
import pyspark.sql.functions as F
from pyspark.sql.functions import col, avg, when
from pyspark.sql.types import FloatType
from pyspark import pandas as pd
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from sklearn.preprocessing import StandardScaler
import numpy as np
import warnings
import statsmodels.api as sm
from sklearn.feature_selection import RFE
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, classification_report, accuracy_score
import joblib

spark = SparkSession.builder \
    .appName("Linear Regression") \
    .getOrCreate()

df = spark.read.format("csv").option("header", "True").load('hdfs://<YOUR IP>:9000/input_heart_attack/heart_attack_dataset2.csv')
df = df.select(
    *(F.col(c).cast('float').alias(c) for c in df.columns))

df = df.toPandas()
print(df)
df.info()
df.describe()

df_X= df.loc[:, df.columns != 'target']
df_y= df.loc[:, df.columns == 'target']

selected_features=[]
lr=LogisticRegression()
rfe=RFE(lr)

warnings.simplefilter('ignore')
rfe.fit(df_X.values,df_y.values)
print(rfe.support_)
print(rfe.ranking_)

for i, feature in enumerate(df_X.columns.values):
    if rfe.support_[i]:
        selected_features.append(feature)

df_selected_X = df_X[selected_features]
df_selected_y=df_y

lm=sm.Logit(df_selected_y,df_selected_X)
result = lm.fit()

print(result.summary2())
warnings.simplefilter('ignore')

X_train,X_test, y_train, y_test=train_test_split(df_X,df_y, test_size = 0.25, random_state =0)
columns = X_train.columns

def cal_accuracy(y_test, y_predict):

    print("\nConfusion Matrix: \n",
    confusion_matrix(y_test, y_predict))

    print (f"\nAccuracy : {accuracy_score(y_test,y_predict)*100:0.3f}")


lr=LogisticRegression()
lr.fit(X_train,y_train)
y_predict=lr.predict(X_test)
print(f"Accuracy of Test Dataset: {lr.score(X_test,y_test):0.3f}")
print(f"Accuracy of Train Dataset: {lr.score(X_train,y_train):0.3f}")
warnings.simplefilter('ignore')

y_total_predict = lr.predict(df_X)
y_total_predict = y_total_predict.T
prediction = pd.Series(y_total_predict)
df['prediction']=prediction.values
print(df)

joblib.dump(lr, 'model.joblib')
df.to_csv('linear_regression_prediction.csv')
