import yaml

from prefect import task

from definitions import CONFIG_PATH


@task
def get_configs():
    with open(CONFIG_PATH, 'r') as yml_file:
        configs = yaml.safe_load(yml_file)
    return configs
