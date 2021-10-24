#This script is implemented using Google Colab.

import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct

#Install spark.
!wget -q https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz

#Unzip the spark file to the current folder
!tar xf spark-3.0.0-bin-hadoop3.2.tgz

#Set your spark folder to your system path environment. 
os.environ["SPARK_HOME"] = "/content/spark-3.0.0-bin-hadoop3.2"

#Install findspark using pip
!pip install -q findspark
findspark.init()
spark = SparkSession.builder.master("local[*]").getOrCreate()


==== Finding Password ====
#Read file containing logs for password.
pw_df = spark.read.text("pw_system.log").repartition(1).cache()
pw_df.toPandas()

#Filter out messages from the alpha system.
filter_alpha = pw_df.filter(~pw_df.value.contains("alpha")).toDF("value")

#Filter out messages from the beta system.
filter_beta = filter_alpha.filter(~filter_alpha.value.contains("beta")).toDF("value")

#Query messages that contains "Invalid user" error.
error_msg = filter_beta.filter(filter_beta.value.contains("Invalid user"))

#Obtain message that contains password.
error_msg.head()[0]


==== Finding Server IP ====
#Read file containing logs for server IP.
server_df = spark.read.option('header', True).csv("IP_network.csv").repartition(1).cache()
server_df.toPandas()

#Filter rows that appear on the 7th day.
session_days = server_df.filter(server_df["day"].isin([0,7,14,21,28]))

#Server does not appear in a non 7th day.
non_session_days = server_df.filter(~server_df["day"].isin([0,7,14,21,28]))
remove_IPs = list(non_session_days.select("dst").toPandas()['dst'])
server_days = session_days.filter(~session_days["dst"].isin(remove_IPs))

#Remove duplicated rows.
dupli_rem = server_days.dropDuplicates()

#Count number of people that have logged on to each server IP.
freq = dupli_rem.groupBy("dst").agg(countDistinct("src")).toDF("dst", "num_src")

#Strictly 13 personal IPs for server IP.
personal_IPs = freq.where(freq.num_src == 13)

#Show server IP found.
personal_IPs.show()

