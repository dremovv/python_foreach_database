from datetime import datetime, timedelta
from typing import Dict, Tuple
import pandas as pd
import os
import sys

import tqdm
from dataclasses import dataclass

from commons.data_access_layer.mindbox.mindbox_dal_manager import MindboxDALManager
from commons.sql_library.directory import COMMON_SQL_SCRIPTS_DIRECTORY
from commons.data_provider.database_data_provider import DatabaseDataProvider


query_name = '__temp_query'


def get_all_projects():
    with DatabaseDataProvider(
        server='db9.corp.itcd.ru',
        database='Nexus',
        script_directory='',
        dal_manager=MindboxDALManager(),
        secrets='{}',
    ) as nexus_data_provider:
        projects = nexus_data_provider.load(
            'dcrm_projects',
            script_directory_override=COMMON_SQL_SCRIPTS_DIRECTORY
        )
    return projects


def prepare_temp_query(query: str) -> None:
    query_filename = query_name + '.sql'
    with open(query_filename, 'w', encoding='utf-8') as f:
        f.write(query)


def load_temp_query(data_provider: DatabaseDataProvider) -> pd.DataFrame:
    result_set = data_provider.load(query_name)
    data_provider.close_pooled_connection(False)
    return result_set


def run_foreach_project(query: str, projects=None, results: dict = None, use_projects_fraction: float = 1.0) -> dict:
    if projects is None:
        projects = get_all_projects()
    prepare_temp_query(query)
    if results is None:
        results = {}
    effective_projects = projects.sample(frac=use_projects_fraction, replace=False)
    for i, row in tqdm.tqdm(effective_projects.iterrows(), total=len(effective_projects)):
        db_name = row['databaseName']
        server_host_name = row['serverHostName']
        print(db_name, file=sys.stderr)
        if db_name not in results:
            results[db_name] = run_query_for_database(db_name, server_host_name, results)
    return results


def merge_results(results):
    results_as_list = []
    for db_name, result in results.items():
        if result is not None:
            result = result.copy()
            result['database'] = db_name
            results_as_list.append(result)
    return pd.concat(results_as_list, ignore_index=True)


def run_query_for_database(db_name: str, server_host_name: str, results: dict) -> pd.DataFrame:
    with DatabaseDataProvider(
            server=server_host_name,
            database=db_name,
            script_directory=os.getcwd(),
            secrets={}
    ) as data_provider:
        try:
            return load_temp_query(data_provider=data_provider)
        except Exception as e:
            print(f'SKIP {db_name}: exeption', e, file=sys.stderr)