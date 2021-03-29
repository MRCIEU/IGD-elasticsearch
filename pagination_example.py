import argparse
import os
import time
from elasticsearch import Elasticsearch
from elasticsearch import helpers

parser = argparse.ArgumentParser(description='Range query.')
parser.add_argument('-e,--ehost', dest='ehost', help='elasticsearch host', default='localhost')
parser.add_argument('-p,--port', dest='port', help='elasticsearch port', default='9200')

args = parser.parse_args()
print(args)

es = Elasticsearch(
    [{'host': args.ehost,'port': args.port}],
)
c = es.cluster.health(wait_for_status='yellow', request_timeout=1)
print(c)

snp_list_file = 'snp-list.txt'

def get_test_snps():
    if os.path.exists(snp_list_file):
        return
    res = es.search('ieu-a',size=100000,request_timeout=1000)
    snp_list = []
    for r in res['hits']['hits']:
        snp = r['_source']['snp_id']
        snp_list.append(snp)
    print(len(snp_list))
    o = open(snp_list_file,'w')
    snp_list = list(set(snp_list))
    for s in snp_list:
        o.write(f'{s}\n')
    o.close()

def read_snps():
    with open(snp_list_file) as f:
        snps = f.read().splitlines()
        print(snps[:10])
        return snps

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def make_multi_body_text(filterData):
    m = {
        "size":10000,
        "query": {
            "bool" : {
                "filter" : filterData
                }
            }
        }
    return m

def elastic_search_multi(bodyText):
    res = es.msearch(
        body=bodyText,request_timeout=300)
    return res

def multi(snp_list,index_name,gwas_list):
    res = []
    request = []
    for c in chunks(snp_list,10000):
        print(len(c))
        req_head = {'index': index_name, "timeout":300, "ignore_unavailable":True}
        filterData = []
        filterData.append({"terms": {'snp_id': c}})
        filterData.append({"terms": {'gwas_id': gwas_list}})
        bodyText=make_multi_body_text(filterData)
        request.extend([req_head, bodyText])
    #print(request)
    start = time.time()
    e = elastic_search_multi(request)
    for response in e['responses']:
        for r in response['hits']['hits']:
            #print(r['_source'])
            res.append(r['_source'])
    end = time.time()
    t = round((end - start), 4)
    print("Time taken: " + str(t) + " seconds")
    print('ES returned ' + str(len(res)) + ' records')
    #print(res)

get_test_snps()
snp_list = read_snps()
#scan(snp_list)
multi(snp_list,'ieu-b',gwas_list=['1','2'])
