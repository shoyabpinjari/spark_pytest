from test_cases.conftest import read_file
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Pytest ETL").getOrCreate()

source = r"C:\Users\Admin\PycharmProjects\pythonProject\spark_pytest\files\Employees_Source.csv"
target = r"C:\Users\Admin\PycharmProjects\pythonProject\spark_pytest\files\Employees_Target.csv"

def test_data_compare():
    # Read source file
    df = read_file(source)

    # Perform count check
    source_count = df.count()
    print('Source count:', source_count)
    print(f"Row count: {source_count}")

    # Read target file
    df = read_file(target)

    # Perform count check
    target_count = df.count()
    print('Target count:', target_count)
    print(f"Row count: {target_count}")

    print('source_count - target_count:',source_count - target_count)
    print('target_count - source_count:',target_count - source_count)