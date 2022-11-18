
from monitor_solution.transformation import Method
from monitor_solution.Logger import Logger


class Monitor:
    def __init__(self):
        self.log = Logger().get_logs()
        self.me = Method()

    def get_data_processed(self, schema_name):
        """Get number of data processed in each stage

        Args:
            schema_name (string): Name of the schema
        """
        tables = self.me.show_table(schema_name)
        tables.show(truncate=False)
        tables_collection = tables.collect()
        for table in tables_collection:
            table_name = table['tableName']
            table_full_name = f"{schema_name}.{table_name}"
            print(table_full_name)
            sql = f"select count(*) cnt from {table_full_name}"
            result_records_status = self.me.show_dataframe(sql).toPandas()
            result_records_status['table_name'] = table_full_name
            result_records_status['msg_type'] = "tracking"
            for i, row in result_records_status.iterrows():
                message = row.to_json()
                self.log.info(message)
