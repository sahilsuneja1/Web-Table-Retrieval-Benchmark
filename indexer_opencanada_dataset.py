from metadata import *
import pandas as pd
import numpy as np
from collections import defaultdict
from elastic import Elastic
import itertools
import pickle
import json
import gc
import pdb
from collections import Counter
from extract import get_table_entities
import stats_opencanada_dataset as ocd_utils


def reset_global_ret_val():
    global global_ret_val
    global_ret_val = 'SUCCESS'


def set_global_ret_val_fail():
    global global_ret_val
    global_ret_val = 'FAIL'


def update_status_list(status_list_file, sample_filename):
    with open(status_list_file,'a') as fd:
        fd.write(sample_filename+'\n')


def update_status_lists(sample_filename, ret_val, proc_id):
    if proc_id is not None:
        finished_list_file = f'es_finished_list_{proc_id}.txt'
        failed_list_file = f'es_failed_list_{proc_id}.txt'
    else:
        finished_list_file = 'es_finished_list.txt'
        failed_list_file = 'es_failed_list.txt'
    #if global_ret_val == 'FAIL':
    if ret_val is True:
        update_status_list(failed_list_file, sample_filename)
    #elif global_ret_val == 'SUCCESS':
    else:
        update_status_list(finished_list_file, sample_filename)


def get_status_list(status_list_file):
    status_list = []
    if not os.path.exists(status_list_file):
        return status_list
    with open(status_list_file) as fd:
        status_list = [i.strip() for i in fd.readlines()]
    return status_list


def get_status_lists(proc_id=None):
    if proc_id is not None:
        finished_list_file = f'es_finished_list_{proc_id}.txt'
        failed_list_file = f'es_failed_list_{proc_id}.txt'
    else:
        finished_list_file = 'es_finished_list.txt'
        failed_list_file = 'es_failed_list.txt'
    return get_status_list(finished_list_file), get_status_list(failed_list_file)


def get_tables_metadata(metadata_file):
    tables_metadata = {}
    with open(metadata_file) as f:
        mappings = [json.loads(line) for line in f]
    for mapping in mappings:
        try:
            if 'data_error' in mapping:
                continue
            table_id = mapping['table_id']
            table_id = table_id.replace(' ','_')
            table_name = mapping['table_name']
            column_headers = ' '.join([str(i['name']) for i in mapping['column_headers']])
            tables_metadata[table_id] = {'table_name': table_name, 'column_headers': column_headers}
        except Exception as e:
            print(f"Exceptiion in get_tables_metadata: {e}")
            pdb.set_trace()
    return tables_metadata 


def create_index(index_name = 'opencanada'):
    mappings = {
        "tid": Elastic.notanalyzed_field(),
        "content": Elastic.analyzed_field(),
        "title": Elastic.analyzed_field(),
        "header": Elastic.analyzed_field(),
        "catchall": Elastic.analyzed_field(),
    }
    elastic = Elastic(index_name,timeout = 200)
    elastic.create_index(mappings,force=True)
    return elastic


def index_table(elastic, tables_metadata, table_id, table_content):
    index_content = None
    ret_val = False
    try:
        table_title = tables_metadata[table_id]['table_name'] if table_id in tables_metadata else ''
        table_header = tables_metadata[table_id]['column_headers'] if table_id in tables_metadata else ''
        index_content = {
            "tid": table_id,
            "content": table_content,
            "title": table_title,
            "header":table_header,
            "catchall": ' '.join([table_title,table_header,table_content]),
        }
        elastic.add_doc(table_id,index_content)
        print(f"indexed table_id {table_id}")
        ret_val = True
    except Exception as e:
        print(f"ERROR processing table_id {table_id}: {e}")
        #set_global_ret_val_fail()
    if index_content is not None:
        del index_content
    if table_content is not None:
        del table_content
    return ret_val


def index_tables(table_list=None, proc_id=None):
    elastic = create_index('opencanada')
    dataset_dir = '/gpfs/suneja/opendata_canada/'

    tables_metadata = get_tables_metadata(dataset_dir+'/metadata.jsonl')

    finished_list, failed_list = get_status_lists(proc_id)

    if table_list is None:
        with open(dataset_dir+'/finished_list_all.txt') as fd:
            table_list = [i.strip() for i in fd.readlines()]

    for idx, filename in enumerate(table_list):
        if filename in finished_list:
            continue
        if filename in failed_list:
            continue
        #reset_global_ret_val()
        ret_val = False
        print(f"{idx}: reading filename {filename}")
        try:
            for table_id, table_content in ocd_utils.read_file(dataset_dir+'/tables/'+filename):
                print(f"indexing table_id {table_id}")
                _ret_val = index_table(elastic, tables_metadata, table_id, table_content)
                if _ret_val is True:
                    ret_val = True  #any one works => log success
        except Exception as e:
            print(f"ERROR processing filenae {filename}: {e}")
            #set_global_ret_val_fail()
        gc.collect()
        print('')            
        update_status_lists(filename, ret_val, proc_id)    




if __name__ == '__main__':
    global_ret_val = 'FAIL'
    pdb.set_trace()
    index_tables()


