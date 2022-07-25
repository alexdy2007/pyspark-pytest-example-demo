import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from basictransforms.word_count import do_word_counts

@pytest.fixture
def tempSparkContext():
    return SparkSession.builder \
    .appName("testing example") \
    .getOrCreate()
  

def test_do_word_counts(tempSparkContext):
    """ test word couting
    Args:
        spark_context: test fixture SparkContext
    """
    
    data =  [
        'hello spark',
        'hello again spark spark'
     ]

    df = tempSparkContext.createDataFrame(data, T.StringType()).toDF('sentences')

    df_results = do_word_counts(df)
    results_dict = df_results.rdd.collectAsMap()
   
    expected_results = {'hello':2, 'spark':3, 'again':1}  
    assert results_dict == expected_results