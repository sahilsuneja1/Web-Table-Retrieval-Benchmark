import os
import pathlib
import json
import filetype
import pandas as pd
import numpy as np
import tempfile
import zipfile


def get_query_and_ground_truth_dataset_ids_mappings():
    mappings = {}
    with open('canada_tag_based_ds_only_gt_v2_split_0.jsonl') as f:
        full_mappings = [json.loads(line) for line in f]
    for full_mapping in full_mappings:
        dataset_ids = full_mapping['doc_ids']
        query = ' '.join(full_mapping['query'])
        if query in mappings:
            print("WARN: repreated query found: {query}")
        else:
            mappings[query] = dataset_ids
    #print("Finished")
    return mappings


def get_ground_truth_dataset_ids():
    dataset_ids = []
    queries = []
    with open('canada_tag_based_ds_only_gt_v2_split_0.jsonl') as f:
        full_mappings = [json.loads(line) for line in f]
    for full_mapping in full_mappings:
        dataset_ids += full_mapping['doc_ids']
        query = ' '.join(full_mapping['query'])
        queries.append(query)
    return dataset_ids
     

def get_dataset_table_mapping(ignore_sheets=True):
    id_mappings = {}
    with open('metadata.jsonl') as f:
        full_mappings = [json.loads(line) for line in f]
    for full_mapping in full_mappings:
        dataset_id = full_mapping['dataset_id']
        if ignore_sheets:
            table_id = full_mapping['table_id'].split('#')[0]   #ignore sheet names
        else:
            table_id = full_mapping['table_id']
        table_id = table_id.replace(' ','_')
        if dataset_id not in id_mappings:
            id_mappings[dataset_id] = [table_id]
        else:
            id_mappings[dataset_id].append(table_id)
    return id_mappings
    

def get_table_ids():
    return [pathlib.Path(i).stem for i in os.listdir('tables')]


def compare():
    table_ids = get_table_ids()
    dataset_table_mapping = get_dataset_table_mapping(ignore_sheets=True)
    ground_truth_dataset_ids = get_ground_truth_dataset_ids()

    print(f"len(table_ids) = {len(table_ids)}")
    print(f"len(dataset_table_mapping) = {len(dataset_table_mapping)}")
    print(f"len(ground_truth_dataset_ids) = {len(ground_truth_dataset_ids)}")
    print(f"len(set(ground_truth_dataset_ids)) = {len(set(ground_truth_dataset_ids))}")

    #assert all tables exist
    not_found_ctr = 0
    ctr_mapped_table_ids = 0
    for dataset_id in dataset_table_mapping:
        mapped_table_ids = dataset_table_mapping[dataset_id]
        for table_id in mapped_table_ids:
            ctr_mapped_table_ids += 1
            if table_id not in table_ids:
                #print(f"table_id {table_id} not found")
                not_found_ctr += 1
    print(f"Found all tables except {not_found_ctr} / {ctr_mapped_table_ids}")

    # assert all ground truth dataset ids exist
    not_found_ctr = 0
    for dataset_id in set(ground_truth_dataset_ids):
        if dataset_id not in dataset_table_mapping.keys():
            print(f"dataset_id {dataset_id} not found")
            not_found_ctr += 1
    print(f"Found all ground truth dataset ids except {not_found_ctr}")


    # assert all ground truth tables exist
    not_found_ctr = 0
    ctr_mapped_table_ids = 0
    for dataset_id in set(ground_truth_dataset_ids):
        mapped_table_ids = dataset_table_mapping[dataset_id]
        for table_id in mapped_table_ids:
            ctr_mapped_table_ids += 1
            if table_id not in table_ids:
                #print(f"table_id {table_id} not found")
                not_found_ctr += 1
    print(f"Found all ground truth tables except {not_found_ctr}  / {ctr_mapped_table_ids}")


def analyze_table_names():
    table_names = []
    dataset_names = []
    with open('metadata.jsonl') as f:
        full_mappings = [json.loads(line) for line in f]
    for full_mapping in full_mappings:
        table_names.append(full_mapping['table_name'])
        dataset_names.append(full_mapping['dataset_name'])
    #print(f"len(set(table_names)) = {len(set(table_names))}")
    #for i in set(table_names):
    #    print(i)
    print(f"len(set(dataset_names)) = {len(set(dataset_names))}")
    for i in set(dataset_names):
        print(i)
    

def emit_query(ctr, query):
    with open('queries.txt','a') as fd:
        fd.write(f"{ctr} {query}\n")


def emit_qrel(ctr, tables):
    with open('qrels.txt','a') as fd:
        for table in tables:
            fd.write(f"{ctr}\t0\t{table}\t2.0\n")


def reset_query_qrel():
    files = ['queries.txt', 'qrels.txt']
    for file in files:
        if os.path.exists(file):
            os.remove(file)


def extract_queries_and_qrels():
    dataset_table_mappings = get_dataset_table_mapping(ignore_sheets=False)
    query_datasetid_mappings = get_query_and_ground_truth_dataset_ids_mappings()

    reset_query_qrel()

    for ctr, (query, dataset_ids) in enumerate(query_datasetid_mappings.items(), start=1):
        emit_query(ctr, query)
        for dataset_id in dataset_ids:
            tables = dataset_table_mappings[dataset_id] 
            emit_qrel(ctr, tables)


def read_file_csv(filepath):
    try:
        df = pd.read_csv(filepath, header=None, skipinitialspace=True, encoding="utf-8-sig", sep=None, engine='python')
    except UnicodeDecodeError:
        df = pd.read_csv(filepath, header=None, skipinitialspace=True, encoding="latin1", sep=None, engine='python')
    df = df.replace({np.nan: ' '})
    #content = ' '.join(list(df.values.flatten()))
    content = ' '.join(list(df.values.astype(str).flatten()))
    print(content)

def read_file_xls(filepath):
    pass
def read_file_xlsx(filepath):
    pass

def read_file_zip(filepath):
    with tempfile.TemporaryDirectory(dir='.') as tmpdirname:
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            zip_ref.extractall(tmpdirname)
            filenames = os.listdir(tmpdirname)
            if len(filenames) != 2:
                print(f"WARN: potentially problematic zip file {filepath}")
            for filename in filenames:
                if not filename.endswith('.csv'):
                    print(f"WARN: potentially problematic zip file {filepath}")
                    continue    
                if 'metadata' not in filename.lower():
                    print(f"{filename}")
                    read_file_csv(tmpdirname+'/'+filename)


def read_files():
    for filename in os.listdir('tables'):
        filepath = 'tables/'+filename
        suffix = pathlib.Path(filepath).suffix.lower()
        if suffix == '.xls':
            read_file_xls(filepath)
        elif suffix == '.xlsx' or suffix == '.xlsm':
            read_file_xlsx(filepath)
        elif suffix == '.csv':
            file_type = filetype.guess(filepath)
            if not file_type:
                read_file_csv(filepath)
            elif file_type.extension == 'zip':
                read_file_zip(filepath)
            else:
                print(f"Can't read file {filepath}")
        else:
            print(f"Can't read file {filepath}")
        
#compare()
#analyze_table_names()
#get_query_and_ground_truth_dataset_ids_mappings()
#extract_queries_and_qrels()

#import pdb
#pdb.set_trace()
#read_file_csv('tables/0000c816-d29f-4cb3-8255-6a32869a00b8.CSV')
#read_file_csv('tables/0004c0c9-beb5-49fb-ae73-fb5d9e829d99.CSV')
#read_file_zip('tables/0004a3f3-8c27-4140-a881-7bd44b2ec5bf.CSV')
#read_file_zip('tables/0009c280-a1ed-4412-8ae9-fe5ea157ce6e.CSV')
#read_file_csv('tmp/13100342.csv')

#read_files()
