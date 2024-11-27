from pyspark.sql import SparkSession
jar = r"C:\Users\Admin\PycharmProjects\pythonProject\spark_pytest\jar\ojdbc8-21.5.0.0.jar"

spark = SparkSession.builder.master("local[1]") \
    .appName("test")\
    .config("spark.jars", jar)\
    .config("spark.driver.extraClassPath", jar)\
    .config("spark.executor.extraClassPath", jar)\
    .getOrCreate()


df = (spark.read.format("jdbc")
    .option("url","jdbc:oracle:thin:@//localhost:1521/XEPDB1")
    .option("query","select * from departments")      # .option("dbtable","employees")
    .option("user","hr")
    .option("password","hr")
    .option("driver","oracle.jdbc.driver.OracleDriver")
    .load())

df.show()

### UPDATED BY SWAPNIL
## Connection



#### version2 created