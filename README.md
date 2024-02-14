# bigData-HeartAttack
## Introduction
The main goal of this project is to predict the risk attack of a patient using big data tools. To do so we are using datasets which includes different significant health value as well as the target (0=no risk of heart attack and 1=Risk of heart attack) for each patient. Then, using correlation matrix and linear regression we are creating a model based on the data that allows the prediction of heart attack risk of any patient entered as an input. After, we apply this model to two streaming contexts : In the first one a file with data for different patient is used as an input and the output gives the risk for each line(i.e each patient), the second is a socket streaming where the input is a patient data we enter in the port and the output gives ‘at risk’ or ‘no risk’.
### rdd_dataset1.py 
This script serves as the starting point in data processing. Its main objective is to clean and prepare the heart attack dataset for an easier analysis. Certain columns that are not necessary for analysis or modeling, such as Patient ID, Country, Continent, and Hemisphere, are removed. Categorical values are replaced with numerical representations for gender and health status, facilitating subsequent processing and modeling. After applying the necessary transformations, the resulting dataset is saved for further analysis or modeling. 
### correlation_matrix.py 
The correlation matrix is calculated to understand the relationship between different attributes and the risk of a heart attack, and to be able to compare our initial dataset (dataset1) with a newer, more practical one (dataset2) for modeling purposes. It computes the correlation coefficients between each attribute and the target variable (Heart Attack Risk), helping identify which attributes are most related to the risk of a heart attack.
### linear_regression_model.py 
This script focuses on modeling and predicting heart attack risks using linear logistic regression. Recursive Feature Elimination (RFE) is used to select the most important features for predicting the risk of a heart attack. A linear logistic regression model is trained using the selected features and saved using joblib.
### stream_from_file.py and stream_from_socket.py 
Designed to process data in real-time and make predictions about heart attack risks. Using Spark Streaming to handle continuous data streams. They initialize a Spark streaming context to listen for incoming data from HDFS directory in the first case and from a socket in the second case. They load the pre-trained logistic regression model to make real-time predictions on patient data. 

## prerequisite
To run this project you will need an hadoop environnement with spark.

Here, we are using AWS. If it is not your cas please adapt the following commands to your case. 

To avoid any error, we recommand to use the following versions : 
- hadoop : version 2.10.2
- spark : version 3.5.0
- python : version 3.10.12

We also recommand not changing any file's name.

## Needed files 
To run this project you first need to downloads some files/directories :

#### 1. /input_heart_attack directory must be added to hdfs. 
It contains 2 datasets :
  - heart_attack_dataset1.txt
  - heart_attack_dataset2.csv

Example of command to load the directory on your virtual machine (depending on environment) : `scp -i '<yourKey>' -r '<path/to/directory>' ubuntu@<Your IP>:<directory>` 

Example of command to put the directory in hdfs :    `hadoop fs -put -d <directory> </directory>`       

#### 2. rdd_dataset1.py
  In this file, please make sure to change the IP by yours at line 19 and 24. For instance, you can use the command `vi rdd_dataset1.py` to access the file and `:set nu` to show line numbers.

  If you are changing any .txt or .csv file's name make sure to change it too in every .py that use this file. 

  Example of command to load the file on your virtual machine (depending on environment) : `scp -i '<yourKey>' '<path/to/file>' ubuntu@<Your IP>:<file>`
  
#### 3. correlation_matrix.py
  In this file, please make sure to change the IP by yours at line 11, 12 and 19. For instance, you can use the command `vi correlation_matrix.py` to access the file and `:set nu` to show line numbers.

  Example of command to load the file on your virtual machine (depending on environment) : `scp -i '<yourKey>' '<path/to/file>' ubuntu@<Your IP>:<file>`
  
#### 4. linear_regression_model.py
   In this file, please make sure to change the IP by yours at line 24. For instance, you can use the command `vi linear_regression_model.py` to access the file and `:set nu` to show line numbers.

   Example of command to load the file on your virtual machine (depending on environment) : `scp -i '<yourKey>' '<path/to/file>' ubuntu@<Your IP>:<file>`

#### 5. patient_to_test.txt
Example of command to load the file on your virtual machine (depending on environment) : `scp -i '<yourKey>' '<path/to/file>' ubuntu@<Your IP>:<file>`

#### 6. stream_from_file.py.
   Example of command to load the file on your virtual machine (depending on environment) : `scp -i '<yourKey>' '<path/to/file>' ubuntu@<Your IP>:<file>`

#### 7. stream_from_socket.py
   Example of command to load the file on your virtual machine (depending on environment) : `scp -i '<yourKey>' '<path/to/file>' ubuntu@<Your IP>:<file>`

## Needed Packages 
To be able to run all the files make sure you installed all the needed packages with the good version
* pandas : version 2.1.4
* scikit-learn : version 1.3.2  
* numpy : version 1.26.3

## Running the project
To run the project you must follow this steps.

#### 1 : You can start by running rdd_dataset1.py 
  Example of commands (depending on your environment) :    `./spark/bin/spark-submit rdd_dataset1.py `
  
  make sure you changed the IP address as mentioned above, you don't already have a directory named 'output_heart_attack' on hdfs and file name / path are corresponding to your environment.
  
  it should return an output_heart_attack/new_dataset1.csv directory with 3 files : SUCCESS, part-00000, and part-00001
  
#### 2 : you can run correlation_matrix.py 

  The point of this file is to return which dataset is usefull for a prediction (next step)

  Example of commands (depending on your environment) :    `./spark/bin/spark-submit correlation_matrix.py `
  
  make sure you changed the IP address as mentioned above and file name / path are corresponding to your environment.
  
  it should return the repsonse to the question : is this dataset intresting for prediction ? as well as the correlation matrix.

#### 3 : You can run linear_regression_model.py

  In this file we are applying a linear regression to heart_attack_dataset1.csv as recommanded by the correlation_matrix.py in order to predict 'target' 

  Example of commands (depending on your environment) :   `./spark/bin/spark-submit linear_regression_model.py `

  make sure you changed the IP address as mentioned above and file name / path are corresponding to your environment.

  It should print info about the linear regression and create to new files : model.joblib where linear regression model is saved (we will use it for the streaming part later) and linear_regression_prediction.csv with results of the prediction.

#### 4. You need to create a new directory 'stream' in hdfs 
  You can use the following command `hadoop fs -mkdir -p /stream`

#### 5. You need to open a new terminal of your master 
  Next step will be to run stream_from_file.py, for that you need to use an other terminal of your master to put/remove file in /stream that will be used for the streaming.

  In our case we are going to use the file patient_to_test.txt

#### 6. Now you can run stream_from_file.py
  Example of commands (depending on your environment) :   `./spark/bin/spark-submit stream_from_file.py`

  While it is running, on your other terminal you can use the following command to read the patient_to_test.txt file in streaming `hdfs dfs -put patient_to_test.txt /stream`

  This will return the risk of heart attack for each patient from the patient_to_test.txt file.

  If you want to run this file again please make sure to remove patient_to_test.txt first usin `hdfs dfs -rm /stream/patient_to_test.txt` then you can repeat previous commands.

#### 7. You need to open a new terminal of your master 
  On this terminal please enter the following commands `nc -lk 9999` which alow to open a socket in listen on your port 9999.

#### 8. You can now run the file stream_from_socket.py
  Example of commands (depending on your environment) :   `./spark/bin/spark-submit stream_from_socket.py`

  While this file is running, you can copy a line from patient_to_test.py and paste it on the terminal where your port 9999 is open.

  It will return 'At risk' if the patient you choose risk an heart attack or 'No risk' if this patient doesn't risk one.
  
