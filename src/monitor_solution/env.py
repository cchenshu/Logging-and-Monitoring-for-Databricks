import os


def isRunningOnDatabricks():
    """
    Check if is running on Databricks

    Returns
    ----------
    bool
    """
    return os.getenv("SPARK_HOME") == "/databricks/spark"
