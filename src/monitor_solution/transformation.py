from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime
from monitor_solution.Logger import Logger
from monitor_solution import env
from pathlib import Path
import tempfile
import shutil
import os


class Method():
    def __init__(self):
        self.log = Logger().get_logs()
        if env.isRunningOnDatabricks():
            self.storage_dir_path = "/mnt/loggingdemo/loandata"
        else:
            self.storage_dir_path = f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/data'

        self.storage_sink_path = f'{tempfile.gettempdir()}/data_storage/csv1'
        self.storage_sink_path2 = f'{tempfile.gettempdir()}/data_storage/csv2'
        print(self.storage_dir_path)

        self.spark = SparkSession.getActiveSession()
        if not self.spark:
            conf = SparkConf().setAppName("data-app").setMaster("local[*]")
            self.spark = SparkSession.builder.config(conf=conf)\
                .config("spark.jars.packages",
                        "io.delta:delta-core_2.12:2.1.0,com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
                .config("spark.driver.memory", "24G")\
                .config("spark.driver.maxResultSize", "24G")\
                .enableHiveSupport()\
                .getOrCreate()

    def addLineage(self, df):
        """Add one column to show the load date

        Args:
            df (dataframe): Target data that will sink to delta table

        Returns:
            dataframe: Spark Dataframe which contains data of the table
        """
        df = df.withColumn("Load_Date", lit(datetime.now())).filter(df.Employee_Name != "Ryan")
        self.log.info("NEW COLUMN HAS BEEN CREATED")
        return df

    def saveasdeltatable(self, df, table_name, table_path, mode="append", overwriteSchema="false"):
        """Sink data as external delta table

        Args:
            df (dataframe): Target data that will sink to delta table
            table_name (string) : Table name with or without schema prefix
            table_path (string): Table name with or without schema prefix
            mode (string):  Defaults to "append".
            overwriteSchema (string): Defaults to "false".
        """
        self.log.info("NEW TABLE HAS BEEN CREATED AND ADDED")
        df.write.format("delta")\
            .mode(mode)\
            .option("overwriteSchema", overwriteSchema)\
            .option("path", table_path)\
            .saveAsTable(table_name)

    def load_csv_file(self, storage_path):
        """Loads data from csv file and returns it as a Dataframe

        Args:
            storage_path (string): File path of the delta table to be loaded

        Returns:
            dataframe: Spark Dataframe which contains data of the table
        """
        self.log.info(f"DATA ({storage_path}) READ STARTS")
        df = self.spark.read.format("csv")\
            .option("header", True)\
            .load(storage_path)
        self.log.info("DATA READ SUCCESSFULLY")
        return df

    def create_schema_if_not_exists(self, schema_name):
        """Creats schema if the given name not exists

        Args:
            schema_name (string): Name of the schema

        Returns:
            string : Name of the schema
        """
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        self.log.info("DATABASE CREATED SUCCESSFULLY")
        return schema_name

    def show_dataframe(self, sql):
        """Show dataframe for the given sql

        Args:
            sql (string): sql string

        Returns:
            dataframe: Spark Dataframe which contains contents
        """
        return self.spark.sql(sql)

    def show_table(self, schema_name):
        """Show tables for the given schema

        Args:
            schema_name (string): Name of the schema

        Returns:
            dataframe: Spark Dataframe which contains name of the table
        """
        return self.spark.sql(f"show tables in {schema_name}")

    def drop_table_if_exists(self, schema_name, table_name, external_table_path):
        """Drop table if it exists

        Args:
            schema_name (string) : Name of the schema
            table_name (string): Table name with or without schema prefix
            external_table_path (string) : Target path to sink the data
        """
        self.spark.sql(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")
        if env.isRunningOnDatabricks():
            external_table_path = f"/dbfs{external_table_path}"

        if Path(external_table_path).exists():
            shutil.rmtree(external_table_path)
            self.log.info("Deleted ADLS path of external table properly.")

    def ingest_transform(self, schema_name, table_name):
        """Ingest and tranform table

        Args:
            schema_name (string): Name of the schema
            table_name (string): Table name with or without schema prefix
        """
        table_path = f'{self.storage_dir_path}/data.csv'

        self.create_schema_if_not_exists(schema_name)
        self.log.info(f"{schema_name} HAS BEEN CREATED")

        df = self.load_csv_file(table_path)
        self.log.info("RAW DATA READS SUCCESSFULLY")
        self.drop_table_if_exists(schema_name, table_name, self.storage_sink_path)
        self.saveasdeltatable(df, f'{schema_name}.{table_name}', self.storage_sink_path)
        self.log.info(f"RAW DATA HAS BEEN ADDED TO {schema_name}")

        df_new = self.addLineage(df)
        new_schema_name = f"{schema_name}_new"
        self.create_schema_if_not_exists(new_schema_name)
        self.log.info(f"{new_schema_name} HAS BEEN CREATED")
        self.drop_table_if_exists(new_schema_name, table_name, self.storage_sink_path2)
        self.saveasdeltatable(df_new, f"{new_schema_name}.{table_name}", self.storage_sink_path2)
        self.log.info(f"PROCESSED DATA HAS BEEN ADDED TO {new_schema_name}")
        return
