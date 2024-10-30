from test_cases.conftest import read_file
from pyspark.sql.functions import *

source = r"C:\Users\Admin\PycharmProjects\pythonProject\spark_pytest\files\Employees_Source.csv"
target = r"C:\Users\Admin\PycharmProjects\pythonProject\spark_pytest\files\Employees_Target.csv"

def test_duplicate_check():
    df = read_file(source)
    print('***** Duplicate in Source *****')
    # Assuming df is your DataFrame, If you want to check for duplicate rows across all columns
    duplicates = df.groupBy(df.columns).count().filter(col("count") > 1)
    duplicates.show()

    # Replace 'col1','col2' with the columns you want to check for duplicates, Check for Duplicate Rows Based on Specific Columns
    duplicates = df.groupBy("employee_id").count().filter(col("count") > 1)
    duplicates.show()

    # Replace 'col1','col2' with the columns you want to check for duplicates,To get the count of duplicate rows
    duplicates = df.groupBy("employee_id").count().filter(col("count") > 1)
    duplicates.show()

    # Show duplicates if they exist
    duplicates = df.groupBy("employee_id").count().filter(col("count") > 1)
    if duplicates.count() > 0:
        print("Duplicates found:")
        duplicates.show()
    else:
        print("No duplicates found.")




def test_duplicate_count_in_target():
    df = read_file(target)
    print('***** Duplicate in Target *****')
    # Assuming df is your DataFrame, If you want to check for duplicate rows across all columns
    duplicates = df.groupBy(df.columns).count().filter(col("count") > 1)
    duplicates.show()

    # Replace 'col1','col2' with the columns you want to check for duplicates, Check for Duplicate Rows Based on Specific Columns
    duplicates = df.groupBy("employee_id").count().filter(col("count") > 1)
    duplicates.show()

    # Replace 'col1','col2' with the columns you want to check for duplicates,To get the count of duplicate rows
    duplicates = df.groupBy("employee_id").count().filter(col("count") > 1)
    duplicates.show()

    # Show duplicates if they exist
    duplicates = df.groupBy("employee_id").count().filter(col("count") > 1)
    if duplicates.count() > 0:
        print("Duplicates found:")
        duplicates.show()
    else:
        print("No duplicates found.")