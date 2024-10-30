import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *

source = r"C:\Users\Admin\PycharmProjects\pythonProject\spark_pytest\files\Employees_Source.csv"


def read_file(file_path):
    spark = SparkSession.builder.master("local").appName("Pytest ETL").getOrCreate()
    if file_path.endswith('.parquet'):
        df = spark.read.parquet(file_path)
    elif file_path.endswith('.csv'):
        df = spark.read.csv(file_path, header=True, inferSchema=True)
    else:
        pytest.fail("Invalid file format")
    return df



def expected_schema(file_path):
    spark = SparkSession.builder.master("local").appName("Pytest ETL").getOrCreate()
    schema = StructType([
        StructField("EMPLOYEE_ID", IntegerType(), True),
        StructField("FULL_NAME", StringType(), True),
        StructField("EMAIL", StringType(), True),
        StructField("SALARY", IntegerType(), True)
    ])
    return schema


def transformation1():
    df = read_file(source)
    # Concatenate first_name and last_name to create full_name, then select specific columns
    transformed_df = df.withColumn("full_name", concat(df["first_name"],df["last_name"])) \
        .select("employee_id", "full_name", "email", "salary")

    return transformed_df


def write_load(load_path, transformed_df):
    if load_path.endswith('.parquet'):
        transformed_df.write.parquet(load_path, mode="overwrite")
    elif load_path.endswith('.csv'):
        transformed_df.write.csv(load_path,mode="overwrite")
    else:
        print("Invalid format")

    return transformed_df
