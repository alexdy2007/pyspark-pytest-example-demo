# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from basictransforms.word_count import do_word_counts
from pyspark.sql import types as T
import os
import sys
import pytest

data =  [
    'hello spark',
    'hello again spark spark'
  ]

df = spark.createDataFrame(data, T.StringType()).toDF('sentences')
df1 = do_word_counts(df)
display(df1)


# COMMAND ----------

# Run all tests in the repository root.
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(notebook_path))
os.chdir(f'/Workspace/{repo_root}')

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

retcode = pytest.main([".", "-p", "no:cacheprovider"])

# Fail the cell execution if we have any test failures.
assert retcode == 0, 'The pytest invocation failed. See the log above for'

# COMMAND ----------


