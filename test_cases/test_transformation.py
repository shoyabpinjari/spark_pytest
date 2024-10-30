from test_cases.conftest import read_file
from pyspark.sql.functions import *
from test_cases.conftest import transformation1


source = r"C:\Users\Admin\PycharmProjects\pythonProject\spark_pytest\files\Employees_Source.csv"
target = r"C:\Users\Admin\PycharmProjects\pythonProject\spark_pytest\files\Employees_Target.csv"

transformed_target = r"C:\Users\Admin\PycharmProjects\pythonProject\spark_pytest\files\Transformed_Employees_Target.csv"
load_path = r"C:\Users\Admin\PycharmProjects\pythonProject\spark_pytest\files\Transformed_Employees_Target.csv"

def test_transformation():
    df = read_file(source)

    transformed_df = transformation1()

    transformed_df.show()

