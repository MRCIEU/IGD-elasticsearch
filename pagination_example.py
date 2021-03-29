import argparse
import os
from elasticsearch import Elasticsearch

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
        lines = f.read().splitlines()
        print(lines[:10])


get_test_snps()
read_snps()
