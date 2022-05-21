from prefect import task, Flow
from prefect.tasks.dbt import DbtShellTask

from prefect.tasks.shell import ShellTask

dir_dbt = 'E:\\Users\\Rushil\\Documents\\Python\\spothist\\spothist'

with Flow('spothist-model') as flow_model:

    result = DbtShellTask(
        profile_name='spothist',
        environment='dev',
        profiles_dir=r'C://Users//Rush//.dbt//',
        #overwrite_profiles=True,
        helper_script=r'cd E:\\Users\\Rushil\\Documents\\Python\\spothist\\spothist',
        shell='shell',
        return_all=True,
        log_stderr=True,
        log_stdout=True,
        # dbt_kwargs={
        #     'type': 'postgres',
        #     'threads': 1,
        #     'host': 'localhost',
        #     'port': 5432,
        #     'user': 'postgres',
        #     'pass': 'postmanpat',
        #     'dbname': 'spothist',
        #     'schema': 'staging',
        # }
    )
    result(command='dbt run')

task = ShellTask(helper_script="cd ~", shell='comm')
with Flow("My Flow") as f:
    # both tasks will be executed in home directory
    contents = task(command='ls')


if __name__ == '__main__':
    flow_model.run()
    #f.run()
