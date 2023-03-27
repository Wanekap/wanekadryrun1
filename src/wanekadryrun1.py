import pyspark
import pyspark.sql.types as T
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import lit
import pyspark.sql.functions as func


from pyspark.sql import functions as F
from pyspark.sql.functions import concat,concat_ws
from pyspark.sql.functions import ceil, col


#postgressql into spark

spark = SparkSession.builder.config("spark.jars","postgresql-42.5.3.jar").master("local").appName("PySpark_Postgres_test").getOrCreate()
df = spark.read.format("jdbc").option("url","jdbc:postgresql://ec2-13-40-49-105.eu-west-2.compute.amazonaws.com:5432/testdb").option("driver", "org.postgresql.Driver").option("dbtable", "wanekaterraluna").option("user", "consultants").option("password","WelcomeItc@2022").load()

print(df.printSchema())

#Pyspark Transformation

#intdf = df.select("*",ceil("price")).show()
intdf = df.withColumn("roundedprice", func.round(df["price"], 2))

#csdf = df.withColumn("circulating_supply", col("market_cap') / col("price"))
csdf = df.withColumn("circulating_supply", df.market_cap/df.price)

# Create Hive table
intdf.write.mode('overwrite') \
    .saveAsTable("pythongroup.wanekaterraluna1")

csdf.write.mode('overwrite') \
    .saveAsTable("pythongroup.wanekaterraluna2")
