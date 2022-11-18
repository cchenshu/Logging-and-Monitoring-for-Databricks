import logging
import inspect
from pyspark.sql import SparkSession
from monitor_solution import env
from monitor_solution.spark_log4j_handler import spark_log4j_handler


class Logger:
    def get_logs(self, logLevel=logging.DEBUG):
        self.__log4j_handler = None
        self.logger_name = inspect.stack()[1][3]
        self.logger = logging.getLogger(self.logger_name)
        self.logger.setLevel(logLevel)
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s : %(message)s')
        ch.setFormatter(formatter)

        if not self.logger.hasHandlers():
            self.logger.addHandler(ch)

        if env.isRunningOnDatabricks() and not self.__log4j_handler:
            spark = SparkSession.getActiveSession()
            self.__log4j_handler = spark_log4j_handler(spark)

            if len(self.logger.handlers) < 2:
                self.logger.addHandler(self.__log4j_handler)

        return self.logger
