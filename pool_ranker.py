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


def get_num_ground_truth_hits(qrel_filename):
    gt_hits = {}
    with open(qrel_filename) as fd:
        qrels = fd.readlines()
    for qrel in qrels:
        qrel_items = qrel.split('\t')
        q_id = qrel_items[0]
        hit_id = qrel_items[2]
        if q_id in gt_hits:
            gt_hits[q_id] += 1
        else:
            gt_hits[q_id] = 1
    sorted_gt_hits = sorted(gt_hits.items(), key=lambda kv: kv[1], reverse=True)        
    return gt_hits


def filter_results(q_id, es_results, topn):
    hits = sorted(es_results.items(), key=lambda kv: kv[1], reverse=True)
    if len(hits) < topn:
        print(f"WARN: insufficient hit count for q_id {q_id}")
    return hits[:topn]


def query_opencanada(topn=20):
    es = Elastic(index_name='opencanada')
    wiki_loader = WikiTables('/gpfs/suneja/opendata_canada')
    q_dict = wiki_loader.get_queries()
    qrel_ground_truth_hit_counts = get_num_ground_truth_hits('/gpfs/suneja/opendata_canada/qrels_filtered.txt')
    queries = [es.analyze_query({'text': q_dict[q]}) for q in q_dict]
    #fields = ['content', 'catchall']
    fields = ['catchall']
    queries = [queries[0]]
    for field in fields:
        rs = es.bulk_search(queries,field)
        #generate result file
        f_rank = open('opencanada_query_results_'+field+'.txt', 'w')
        for q_id, query in enumerate(queries):
            rank = 1
            query_id_str = str(q_id+1)
            if query_id_str not in qrel_ground_truth_hit_counts:    #queries with no hits post filtering/indexing
                continue
            #rs = filter_results(q_id, rs[q_id], qrel_ground_truth_hit_counts[query_id_str])
            rs = filter_results(q_id, rs[q_id], topn)
            for each_rs in rs.items():
                f_rank.write(query_id_str + "\tQ0\t" + each_rs[0] + "\t" + str(rank) + "\t" + str(
                    each_rs[1]) + "\t" + field + "\n")
                rank += 1
        f_rank.close()


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
   pdb.set_trace()
   query_opencanada()
