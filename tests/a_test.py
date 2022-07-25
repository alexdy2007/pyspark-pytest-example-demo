import pytest
from pyspark.sql import SparkSession

from basictransforms.transform import multiply_by_n

@pytest.fixture
def tempSparkContext():
    return SparkSession.builder \
    .appName("testing example") \
    .getOrCreate()

# Arrange
@pytest.fixture
def test_data(tempSparkContext):
 
  df = tempSparkContext.createDataFrame(
    [
        (1, "foo",4),  # create your data here, be consistent in the types.
        (2, "bar",5),
        (3, "baz",6),  # create your data here, be consistent in the types.
        (4, "pop",7)
    ],
    ["id", "label", 'num']  # add your column names here
  )
  return df

def test_transform(test_data):
    # Act
    df=multiply_by_n(test_data,'num',3)
    pdf=df.toPandas()
    
    first_row_num = pdf['num'][0]
    assert first_row_num == 12