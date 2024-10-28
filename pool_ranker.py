'''
unsupervised baselines:
1. BM25 for different fields
2. MLM

Output
    1. trec formated result file from each baseline
    2. raw table content from pooled results
'''
from scorer import ScorerMLM
from elastic import Elastic
from metadata import *
from data_loader import WikiTables
from glob import glob
import json
import pdb
import stats_opencanada_dataset as ocd_utils

def query_elasticsearch_example():
    es = Elastic(index_name=webtable_index_name)
    wiki_loader = WikiTables('./data')
    q_dict = wiki_loader.get_queries()
    queries = [es.analyze_query({'text': q_dict[q]}) for q in q_dict]
    queries = [queries[0]]
    fields = ['content']
    topn=20
    for field in fields:
        import pdb
        pdb.set_trace()
        rs = es.bulk_search(queries,field)
        for q_id, query in enumerate(queries):
            print(query)
            for each_rs in sorted(rs[q_id].items(), key=lambda kv: kv[1], reverse=True)[:topn]:
                print(each_rs[0], each_rs[1])


def run_WDC_singleField(topn=20):
    es = Elastic(index_name=webtable_index_name)
    wiki_loader = WikiTables('./data')
    q_dict = wiki_loader.get_queries()
    queries = [es.analyze_query({'text': q_dict[q]}) for q in q_dict]
    #fields = ['content','textBefore','textAfter','pageTitle','title','header','catchall']
    fields = ['content']
    for field in fields:
        rs = es.bulk_search(queries,field)
        #generate result file
        f_rank = open(os.path.join(wdc_rank_path,field+'.txt'), 'w')
        for q_id, query in enumerate(queries):
            rank = 1
            for each_rs in sorted(rs[q_id].items(), key=lambda kv: kv[1], reverse=True)[:topn]:
                f_rank.write(str(q_id+1) + "\tQ0\t" + each_rs[0] + "\t" + str(rank) + "\t" + str(
                    each_rs[1]) + "\t" + field + "\n")
                rank += 1
        f_rank.close()


def filter_results(q_id, es_results, topn):
    hits = sorted(es_results.items(), key=lambda kv: kv[1], reverse=True)
    if len(hits) < topn:
        print(f"WARN: insufficient hit count for q_id {q_id}")
    return hits[:topn]


def emit_qrel(f_rank, query_id_str, search_result, index_field):
    #f_rank = open('opencanada_query_results_'+index_field+'.txt', 'a')
    rank = 1
    for each_rs in search_result:
        f_rank.write(query_id_str + "\tQ0\t" + each_rs[0] + "\t" + str(rank) + "\t" + str(
            each_rs[1]) + "\t" + index_field + "\n")
        rank += 1
    f_rank.flush()
    #f_rank.close()
        

def reset_qrel_file(index_field):
    f_rank = open('opencanada_query_results_'+index_field+'.txt', 'w')
    f_rank.close()


def query_opencanada_individual(topn=20):
    es = Elastic(index_name='opencanada')
    qrel_ground_truth_hit_counts = ocd_utils.get_num_ground_truth_hits('/gpfs/suneja/opendata_canada/qrels_filtered.txt')

    wiki_loader = WikiTables('/gpfs/suneja/opendata_canada')
    q_dict = wiki_loader.get_queries()

    queries = [es.analyze_query({'text': q_dict[q]}) for q in q_dict]
    #fields = ['content', 'catchall']
    fields = ['catchall']
    #queries = queries[0:200]

    for field in fields:
        f_rank = open('opencanada_query_results_'+field+'.txt', 'w')
        for q_id, query in enumerate(queries):
            search_result = es.search(query,field)
            query_id_str = str(q_id+1)
            if query_id_str not in qrel_ground_truth_hit_counts:    #queries with no hits post filtering/indexing
                continue
            #topn = qrel_ground_truth_hit_counts[query_id_str]    
            search_result = filter_results(query_id_str, search_result, topn)
            emit_qrel(f_rank, query_id_str, search_result, field)
        f_rank.close()            


def query_opencanada_allatonce(topn=20):
    es = Elastic(index_name='opencanada')
    qrel_ground_truth_hit_counts = ocd_utils.get_num_ground_truth_hits('/gpfs/suneja/opendata_canada/qrels_filtered.txt')

    wiki_loader = WikiTables('/gpfs/suneja/opendata_canada')
    q_dict = wiki_loader.get_queries()

    queries = [es.analyze_query({'text': q_dict[q]}) for q in q_dict]
    #fields = ['content', 'catchall']
    fields = ['catchall']
    queries = queries[0:4]

    for field in fields:
        f_rank = open('opencanada_query_results_'+field+'.txt', 'w')
        #reset_qrel_file(field)
        search_results = es.bulk_search(queries,field)
        for q_id, query in enumerate(queries):
            query_id_str = str(q_id+1)
            if query_id_str not in qrel_ground_truth_hit_counts:    #queries with no hits post filtering/indexing
                continue
            #topn = qrel_ground_truth_hit_counts[query_id_str]    
            search_result = filter_results(query_id_str, search_results[q_id], topn)
            emit_qrel(f_rank, query_id_str, search_result, field)
        f_rank.close()            


def query_chunked(es, queries, field, qrel_ground_truth_hit_counts, start_qid, chunk_size):
    topn = 20
    f_rank = open('opencanada_query_results_'+field+'.txt', 'a')
    search_results = es.bulk_search(queries,field)
    for q_id, query in enumerate(queries):
        query_id_str = str(start_qid+q_id+1)
        if query_id_str not in qrel_ground_truth_hit_counts:    #queries with no hits post filtering/indexing
            continue
        #topn = qrel_ground_truth_hit_counts[query_id_str]    
        search_result = filter_results(query_id_str, search_results[q_id], topn)
        emit_qrel(f_rank, query_id_str, search_result, field)
    f_rank.close()            


def query_opencanada():
    es = Elastic(index_name='opencanada')
    qrel_ground_truth_hit_counts = ocd_utils.get_num_ground_truth_hits('/gpfs/suneja/opendata_canada/qrels_filtered.txt')

    wiki_loader = WikiTables('/gpfs/suneja/opendata_canada')
    q_dict = wiki_loader.get_queries()

    queries = [es.analyze_query({'text': q_dict[q]}) for q in q_dict]
    #fields = ['content', 'catchall']
    fields = ['catchall']
    queries = queries[0:200]
    chunk_size = 100

    for field in fields:
        reset_qrel_file(field)
        for i in range(0, len(queries), chunk_size):
            query_chunked(es,
                          queries[i:i+chunk_size],
                          field,
                          qrel_ground_truth_hit_counts,
                          i,
                          chunk_size)



def list_opencanada_queries_missing_groundtruth():
    fdw = open('/gpfs/suneja/opendata_canada/queries_missing_groundtruth.txt','w')
    with open('/gpfs/suneja/opendata_canada/queries.txt') as fd:
        q_ids = [i.split()[0] for i in fd.readlines()]
    qrel_ground_truth_hit_counts = ocd_utils.get_num_ground_truth_hits('/gpfs/suneja/opendata_canada/qrels_filtered.txt')
    for q_id in q_ids:
        if q_id not in qrel_ground_truth_hit_counts:    #queries with no hits post filtering/indexing
            fdw.write(q_id+'\n')
    fdw.close()        

def collect_pooled_WDC_tables():
    # collect table ids from all result files
    pool_files = glob(os.path.join(wdc_rank_path,'*'))
    top_tids = set()
    for pool_file in pool_files:
        f = open(pool_file,'r')
        for line in f:
            top_tids.add(line.split('\t')[2])
        f.close()

    # get table content from elasticsearch
    f_table = open(os.path.join(wdc_data_path,'wdc_pool.json'),'w')
    es = Elastic(index_name=webtable_index_name)
    for tid in top_tids:
        doc = es.get_doc(tid)
        f_table.write(json.dumps(doc['_source'])+'\n')
    f_table.close()



if __name__  == '__main__':
   #run_WDC_singleField()
   #collect_pooled_WDC_tables()
   #query_elasticsearch_example()
   #pdb.set_trace()
   query_opencanada()
   #query_opencanada_allatonce()
   #query_opencanada_individual()
   #list_opencanada_queries_missing_groundtruth()
