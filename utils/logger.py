import logging
from datetime import datetime

from prefect import task

from definitions import ROOT_DIR


def logger_handler():
    """
    Set up and return custom logging handler to write Prefect logs to file.
    """

    # Specify and set up file handler properties (of the logger handler)
    file_path = ROOT_DIR / 'logs' / 'spothist.log'
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(name)s | %(message)s')

    file_handler = logging.FileHandler(file_path)
    file_handler.setFormatter(formatter)

    # Create logger handler and add specified file handler
    custom_handler = logging.getLogger(__name__)
    custom_handler.addHandler(file_handler)

    return custom_handler


@task
def get_last_run() -> int:
    """
    Extract last successful run's time from log file and convert to unix timestamp in milliseconds.
    """

    file_path = ROOT_DIR / 'logs' / 'spothist.log'

    # Open log file and get most recent run's information
    try:
        with open(file_path, 'r') as log_file:
            lines = log_file.readlines()
        str_last_run = lines[-1]  # Most recent run is logged to last line in file

        # Extract substring between first set of square brackets and replace comma w/ period before milliseconds
        str_last_run = str_last_run[str_last_run.find('[') + 1:str_last_run.find(']')].replace(',', '.')

        # Convert string representation to datetime and convert result to unix timestamp in milliseconds
        timestamp_last_run = int(datetime.fromisoformat(str_last_run).timestamp() * 1000)

    except FileNotFoundError: # TODO: Log this instead
        print('Log file doesn\'t exist.')
        return None

    except IndexError:
        print('Log file is empty.')
        return None

    except ValueError:
        print('Unable to extract time of last successful run from run log.')
        return None

    else:
        # print(str_last_run)
        return timestamp_last_run  # TODO: Check if timestamp is valid (e.g. not in the future)


if __name__ == '__main__':
    get_last_run.run()
