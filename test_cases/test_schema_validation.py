from test_cases.conftest import read_file
from pyspark.sql.functions import *
from test_cases.conftest import expected_schema


source = r"C:\Users\Admin\PycharmProjects\pythonProject\spark_pytest\files\Employees_Source.csv"
target = r"C:\Users\Admin\PycharmProjects\pythonProject\spark_pytest\files\Employees_Target.csv"

transformed_target = r"C:\Users\Admin\PycharmProjects\pythonProject\spark_pytest\files\Transformed_Employees_Target.csv"

def test_schema_validation():
    t = expected_schema(transformed_target)
    actual_df = read_file(transformed_target)
    print('Expected Schema: ',t)
    print('Actual schema: ', actual_df.schema)
    if actual_df.schema == t:
        print('Schemas are matching')
    else:
        print('Schemas are not matching')

