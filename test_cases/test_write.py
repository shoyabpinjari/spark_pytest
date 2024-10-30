from test_cases.conftest import read_file
from pyspark.sql.functions import *
from test_cases.conftest import transformation1
from test_cases.conftest import write_load

source = r"C:\Users\Admin\PycharmProjects\pythonProject\spark_pytest\files\Employees_Source.csv"
target = r"C:\Users\Admin\PycharmProjects\pythonProject\spark_pytest\files\Employees_Target.csv"

transformed_target = r"C:\Users\Admin\PycharmProjects\pythonProject\spark_pytest\files\Transformed_Employees_Target.csv"
load_path = r"C:\Users\Admin\PycharmProjects\pythonProject\spark_pytest\files\Transformed_Employees_Target.csv"

def test_load():
    transformed_df = transformation1()
    write_load(load_path, transformed_df)
