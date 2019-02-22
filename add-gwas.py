#!/usr/bin/env python

import random
import time
import json
import os
import config
import gzip
import ntpath
import sys
import argparse
import logging
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from collections import deque

#elasticsearch
es = Elasticsearch(
    [{'host': config.elastic_host,'port': config.elastic_port}],
)

#main function index_gwas_data requires one required, and one optional paramater
#1. gwas_id (required)
#2. index_name (optional)

def es_gwas_search(gwas_id, index_name):
    res=es.search(
        request_timeout=60,
        index=index_name,
        body={
            "size":1,
            "query": {
                "bool" : {
                    "filter" : [
                        {"term":{"gwas_id":gwas_id}},
                    ]
                }
            }
        })
    total=res['hits']['total']
    #print(res)
    return(total)

def delete_index(index_name):
    if es.indices.exists(index_name):
        print("Deleting '%s' index..." % (index_name))
        res = es.indices.delete(index = index_name)
        print(" response: '%s'" % (res))

def create_index(index_name,shards=5):
    if es.indices.exists(index_name):
        print("Index name already exists, please choose another")
    else:
        print("Creating index "+index_name)
        request_body ={
            "settings":{
                "number_of_shards" : shards,
                "number_of_replicas":1,
                "index.codec": "best_compression",
                "refresh_interval":-1,
                "index.max_result_window": 100000
            },
            "mappings":{
                "_doc" : {
                    "properties": {
                        "gwas_id": { "type": "keyword"},
                        "snp_id": { "type": "keyword"},
                        "effect_allele": { "type": "keyword", "index":"false"},
                        "other_allele": { "type": "keyword", "index":"false"},
                        "effect_allele_freq": { "type": "float", "index":"false"},
                        "p":{"type":"float"},
                        "n":{"type":"float","index":"false"},
                        "beta":{"type":"float", "index":"false"},
                        "se":{"type":"half_float","index":"false"}
                     }
                }
            }
        }
        es.indices.create(index = index_name, body = request_body, request_timeout=60)

def check_gwas(gwas_file,gwas_id,index_name):
    print('Checking',gwas_id,gwas_file)
    #check file exists
    if os.path.exists(gwas_file):
        print('Checking for previously indexed records...')
        #check index exists
        if es.indices.exists(index_name):
            #check no data indexed for this gwas
            total = es_gwas_search(gwas_id,index_name)
            print('Number of existing records = '+str(total))
            if int(total)>0:
                return {'error':'Error: Indexed records exist for '+gwas_id}
            else:
                return gwas_file
        else:
            print('No existing index, so no records :)')
            return gwas_file
    else:
        return {'error':'Error: Can not access file '+gwas_file}


def index_gwas_data(gwas_file, gwas_id, index_name):
    print("Indexing gwas data...")

    #set up logging
    formatter=logging.Formatter('%(asctime)s %(msecs)d %(threadName)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s',datefmt='%d-%m-%Y:%H:%M:%S')
    #logging.basicConfig(filename=study_name.replace(':','_')+'.log',level=logging.INFO)
    handler = logging.FileHandler('/data/'+gwas_id.replace(':','_')+'.log')
    handler.setFormatter(formatter)

    logger = logging.getLogger(gwas_id)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    #do some checks
    check_result = check_gwas(gwas_file,gwas_id,index_name)
    #print(check_result)
    if 'error' in check_result:
        print(check_result['error'])
    else:

        #remove index from gwas_id
        if ':' in gwas_id:
            gwas_id = gwas_id.split(':')[1]

        #print gwas_data,index_name
        if es.indices.exists(index_name):
            print("Index already exists, adding to that one then :)")
        else:
            create_index(index_name)
        bulk_data = []
        counter=0
        start = time.time()
        chunkSize = 100000
        with gzip.open(gwas_file) as f:
            #next(f)
            for line in f:
                counter+=1
                if counter % 100000 == 0:
                    end = time.time()
                    t=round((end - start), 4)
                    print(gwas_file,t,counter)
                if counter % chunkSize == 0:
                    deque(helpers.streaming_bulk(client=es,actions=bulk_data,chunk_size=chunkSize,request_timeout=60),maxlen=0)
                    bulk_data = []
                #print(line.decode('utf-8'))
                l = line.rstrip().decode('utf-8').split(' ')
                #print(l)
                if l[0].startswith('rs'):
                    effect_allele_freq = beta = se = p = n = ''
                    try:
                        effect_allele_freq = float(l[3])
                    except ValueError:
                        #print(l)
                        logger.info(l[0]+' '+str(gwas_id)+' '+gwas_file+' '+str(counter)+' effect_allele_freq error')
                    try:
                        beta = float(l[4])
                    except ValueError:
                        logger.info(l[0],gwas_id,gwas_file,counter,'beta error')
                    try:
                        se = float(l[5])
                    except ValueError:
                        logger.info(l[0],gwas_id,gwas_file,counter,'se error')
                    try:
                        p = float(l[6])
                    except ValueError:
                        logger.info(l[0],gwas_id,gwas_file,counter,'p error')
                    try:
                        n = float(l[7].rstrip())
                    except ValueError:
                        logger.info(l[0],gwas_id)
                    data_dict = {
                            'gwas_id':gwas_id,
                            'snp_id':l[0],
                            'effect_allele':l[1],
                            'other_allele':l[2],
                            'effect_allele_freq':effect_allele_freq,
                            'beta':beta,
                            'se':se,
                            'p':p,
                            'n':n
                    }
                    op_dict = {
                        "_index": index_name,
                        #"_id" : gwas_id+':'+l[0],
                        "_op_type":'create',
                        "_type": '_doc',
                        "_source":data_dict
                    }
                    bulk_data.append(op_dict)
                    #bulk_data.append(data_dict)
        #print bulk_data[0]
        #print len(bulk_data)
        deque(helpers.streaming_bulk(client=es,actions=bulk_data,chunk_size=chunkSize,request_timeout=60),maxlen=0)
        #refresh the index
        es.indices.refresh(index=index_name)
        total = es_gwas_search(gwas_id,index_name)
        print('Records indexed: '+str(total))
        logger.info('Records indexed: '+str(total))

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Add a new GWAS study.')
    parser.add_argument('-m,--method', dest='method', help='(create_index, delete_index, index_data)')
    parser.add_argument('-i,--index_name', dest='index_name', help='the index name')
    parser.add_argument('-g,--gwas_id', dest='gwas_id', help='the GWAS id')
    parser.add_argument('-f,--gwas_file', dest='gwas_file', help='the GWAS file')

    args = parser.parse_args()
    print(args)
    if args.method == None:
        print("Please provide a method (create_index, delete_index, index_data)")
    else:
        if args.method == 'create_index':
            if args.index_name == None:
                print('Please provide an index name (-i)')
            else:
                print('creating index')
                create_index(args.index_name)
        elif args.method == 'delete_index':
            if args.index_name == None:
                print('Please provide an index name (-i)')
            else:
                print('deleting index')
                delete_index(args.index_name)
        elif args.method == 'index_data':
            if args.gwas_id == None:
                print('Please provide a gwas id (-g)')
                exit
            if args.index_name == None:
                print('Please provide an index name (-i)')
                exit
            if args.gwas_file == None:
                print('Please provide a gwas file (-f)')
                exit
            else:
                index_gwas_data(gwas_file=args.gwas_file, gwas_id=args.gwas_id, index_name = args.index_name)
        else:
            print("Not a good method")
