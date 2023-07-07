# Databricks notebook source
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from basictransforms.word_count import do_word_counts

@pytest.fixture
def dataFixture():
    data =  [
        'hello spark',
        'hello again spark spark'
     ]

    return tempSparkContext.createDataFrame(data, T.StringType()).toDF('sentences')
  

def test_do_word_counts(dataFixture):
    """ test word couting
    Args:
        tempSparkContext: test fixture SparkSession
    """
    
    df_results = do_word_counts(dataFixture)
    results_dict = df_results.rdd.collectAsMap()
   
    expected_results = {'hello':2, 'spark':1, 'again':1}  
    print(expected_results)
    assert results_dict == expected_results, 'error'

# COMMAND ----------


import os
import sys 


notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(notebook_path))
os.chdir(f'/Workspace/{repo_root}')

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

retcode = pytest.main([".", "-p", "no:cacheprovider"])

# Fail the cell execution if we have any test failures.
assert retcode == 0, 'The pytest invocation failed. See the log above for'

# COMMAND ----------


