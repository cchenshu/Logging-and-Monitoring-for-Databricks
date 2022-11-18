
from monitor_solution.monitor import Monitor


def main():
    monitor = Monitor()
    monitor.get_data_processed("loan")
    monitor.get_data_processed("loan_new")


if __name__ == "__main__":
    main()
