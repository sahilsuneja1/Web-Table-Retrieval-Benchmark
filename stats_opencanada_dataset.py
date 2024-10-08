import os
import gc
import pathlib
import json
import filetype
import pandas as pd
import numpy as np
import tempfile
import zipfile
import shutil


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
        if 'data_error' in full_mapping:
            continue
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
    

def get_tables_with_metadata():
    tables_with_metadata = set()
    dataset_to_table_mappings =  get_dataset_table_mapping()
    for table_ids in dataset_to_table_mappings.values():
        for table_id in table_ids:
            tables_with_metadata.add(table_id)
    return tables_with_metadata
            

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


def dataframe_to_str(df):
    df = df.replace({np.nan: ''})
    #content = ' '.join(list(df.values.flatten()))
    content = ' '.join(list(df.values.astype(str).flatten()))
    del df
    return content


def read_file_csv(filepath):
    try:
        df = pd.read_csv(filepath, header=None, skipinitialspace=True, encoding="utf-8-sig", sep=None, engine='python')
    except UnicodeDecodeError:
        df = pd.read_csv(filepath, header=None, skipinitialspace=True, encoding="latin1", sep=None, engine='python')
    content = dataframe_to_str(df)    
    table_id = os.path.splitext(os.path.split(filepath)[-1])[0]
    print(table_id)
    #print(content)
    del content
    del df

        
def read_excel_sheet(filepath, engine, sheet_name):
    df = pd.read_excel(filepath, engine=engine, sheet_name=sheet_name, header=None)
    content = dataframe_to_str(df)    
    del df
    return content


def read_excel_file(filepath, engine):    
    df = pd.ExcelFile(filepath, engine=engine)
    table_id = os.path.splitext(os.path.split(filepath)[-1])[0]
    if len(df.sheet_names) == 1:
        content = read_excel_sheet(filepath, engine, sheet_name=df.sheet_names[0])
        print(table_id)
        #print(content)
        del content
    else:    
        for sheet_name in df.sheet_names:
            table_id = table_id + '#' + sheet_name.replace(' ','_')
            content = read_excel_sheet(filepath, engine, sheet_name)
            print(table_id)
            #print(content)
            del content
    del df        
       

def read_file_xlsx(filepath):    
    read_excel_file(filepath, engine='openpyxl')


def read_file_xls(filepath):
    read_excel_file(filepath, engine='xlrd') 


def read_file_zip_strict(filepath):
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
                    table_id = os.path.splitext(os.path.split(filepath)[-1])[0]
                    print(table_id)
                    #print(f"{filename}")
                    read_file_csv(tmpdirname+'/'+filename)


def read_file_zip(filepath, tables_with_metadata=None):
    with tempfile.TemporaryDirectory(dir='.') as tmpdirname:
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            zip_ref.extractall(tmpdirname)
            filenames = os.listdir(tmpdirname)
            for filename in filenames:
                if 'metadata' in filename.lower():
                    continue
                table_id = os.path.splitext(os.path.split(filepath)[-1])[0]
                print(table_id)
                try:
                    read_file_csv(tmpdirname+'/'+filename)
                except Exception as e:
                    if not tables_with_metadata or table_id in tables_with_metadata: 
                        print(f"ERROR reading {filename}: {e}")
                    else:
                        print(f"IGNORE reading {filename}: {e}")


def read_files_trust_extension():
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
                print(f"ERROR: Can't read file {filepath}")
        else:
            print(f"ERROR: Can't read file {filepath}")


def _read_file(filepath, inferred_extension):
    if inferred_extension == 'xls':
        read_file_xls(filepath)
    else: #inferred_extension == 'xlsx':
        read_file_xlsx(filepath)
       

def rename_and_read_tmpfile(filepath, suffix, inferred_extension):
    with tempfile.NamedTemporaryFile(dir='.', suffix='.'+inferred_extension) as fp:
        shutil.copyfile(filepath, fp.name)
        _read_file(fp.name, inferred_extension)


def rename_and_read_file(src_filepath, suffix, inferred_extension):
    filename = os.path.splitext(os.path.split(src_filepath)[-1])[0]
    dst_dir = 'renamed_files/'
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)
    dst_filepath = dst_dir + filename + '.' + inferred_extension 
    shutil.copyfile(src_filepath, dst_filepath)
    _read_file(dst_filepath, inferred_extension)


def read_file(filepath, tables_with_metadata=None):
    suffix = pathlib.Path(filepath).suffix.lower()[1:]
    file_type = filetype.guess(filepath)

    if not file_type:
        if suffix == 'csv':
            return read_file_csv(filepath)
        else:
            print(f"ERROR: Can't read file {filepath}")
            return

    inferred_extension = file_type.extension
    if inferred_extension == 'zip':
        return read_file_zip(filepath, tables_with_metadata)

    if 'xls' not in inferred_extension:
        print(f"ERROR: Can't read file {filepath}")
        return

    if inferred_extension != suffix:
        rename_and_read_file(filepath, suffix, inferred_extension)
    else:
       _read_file(filepath, inferred_extension)



def read_files():
    tables_with_metadata = get_tables_with_metadata()
    for idx, filename in enumerate(os.listdir('tables')):
        print(f"{idx}: reading {filename}")
        try:
            filepath = 'tables/'+filename
            read_file(filepath, tables_with_metadata)
        except Exception as e:
            table_id = os.path.splitext(os.path.split(filepath)[-1])[0]
            if table_id in tables_with_metadata: 
                print(f"ERROR reading {filepath}: {e}")
            else:
                print(f"IGNORE reading {filepath}: {e}")
        gc.collect()
        print('')

        
#compare()
#analyze_table_names()
#get_query_and_ground_truth_dataset_ids_mappings()
#extract_queries_and_qrels()

import pdb
pdb.set_trace()
#read_file_csv('tables/0000c816-d29f-4cb3-8255-6a32869a00b8.CSV')
#read_file_csv('tables/0004c0c9-beb5-49fb-ae73-fb5d9e829d99.CSV')
#read_file_zip('tables/0004a3f3-8c27-4140-a881-7bd44b2ec5bf.CSV')
#read_file_zip('tables/0009c280-a1ed-4412-8ae9-fe5ea157ce6e.CSV')
#read_file_csv('tmp/13100342.csv')
#read_file_xlsx('tables/000dccd4-45a1-4641-9fdd-cecae186948f.XLSX')
#read_file_xls('tables/6bf2ae97-22b3-4e54-88d4-9beee4faf94e.XLS')

#read_file('tables/019f64ee-6ba3-4a90-a511-ac92649a6e49.XLS')
#read_file('tables/0000c816-d29f-4cb3-8255-6a32869a00b8.CSV')
#read_file('tables/0009c280-a1ed-4412-8ae9-fe5ea157ce6e.CSV')
#read_file('tables/0004c0c9-beb5-49fb-ae73-fb5d9e829d99.CSV')
#read_file('tables/000dccd4-45a1-4641-9fdd-cecae186948f.XLSX')
#read_file('tables/6bf2ae97-22b3-4e54-88d4-9beee4faf94e.XLS')

#read_files()
#read_file('tables/1aa09e73-641f-4089-b65c-0602d56fa849.CSV')
#read_file('tables/914b3f01-7a95-4831-9dd9-42b9ce3a792f.CSV')
#read_file_csv('tmp/99-011-X2011007-nhsenm-301-F.tab')
#read_file('tables/1abd1485-ede3-45d2-b49c-993bb92b0ec2.CSV')
read_file('tables/3865232a-b222-439c-8695-079721fe5e35.XLSX')
