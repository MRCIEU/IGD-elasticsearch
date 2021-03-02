#!/usr/bin/env python

import random
import time
import json
import os
import gzip
import ntpath
import sys
import argparse
import logging
import uuid
from pysam import VariantFile
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from collections import deque
from pathlib import Path
import subprocess

#main function index_gwas_data requires one required, and one optional paramater
#1. gwas_id (required)
#2. index_name (optional)

TIMEOUT=500

def es_gwas_count(gwas_id, index_name):
    res=es.count(
        request_timeout=TIMEOUT,
        index=index_name,
        body={
            "query": {
                "bool" : {
                    "filter" : [
                        {"term":{"gwas_id":gwas_id}},
                    ]
                }
            }
        })
    #total=res['hits']['total']
    #print(res['count'])
    return(res['count'])

def delete_index(index_name):
    index_name = index_name.lower()
    if es.indices.exists(index_name,request_timeout=TIMEOUT):
        print("Deleting '%s' index..." % (index_name))
        res = es.indices.delete(index = index_name,request_timeout=TIMEOUT)
        print(" response: '%s'" % (res))

def create_index(index_name,shards=5):
    index_name = index_name.lower()
    if es.indices.exists(index_name,request_timeout=TIMEOUT):
        print("Index name already exists, please choose another")
    else:
        print("Creating index "+index_name)
        request_body ={
            "settings":{
                "number_of_shards" : shards,
                "number_of_replicas":0,
                "index.codec": "best_compression",
                "refresh_interval":-1,
                "index.max_result_window": 100000,
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
                        "n":{"type":"integer","index":"false"},
                        "beta":{"type":"float", "index":"false"},
                        "se":{"type":"float","index":"false"},
                        "chr":{ "type": "keyword"},
                        "position":{"type":"integer"}
                     }
                }
            }
        }
        es.indices.create(index = index_name, body = request_body, request_timeout=TIMEOUT)

def read_write_index(index_name):
    print(f'Setting {index_name} to read/write')
    #https://elasticsearch-py.readthedocs.io/en/latest/api.html#elasticsearch.client.IndicesClient.put_settings
    #curl -XPUT -H "Content-Type: application/json" http://localhost:9200/$index_name/_settings -d '{"index.blocks.read_only_allow_delete": false}'
    body = {"index.blocks.read_only_allow_delete": False}
    es.indices.put_settings(body=body,index=index_name)

def extract_vcf(gwas_file,gwas_id):
    # Temporary output file:
    tempDir=os.getenv('tmpDir',"/tmp/")
    tempout = os.path.join(tempDir,gwas_id)+"."+str(uuid.uuid4())
    print("Processing vcf to", tempout)

    # Check if Sample size is available as a column
    vcf_in = VariantFile(gwas_file)
    sample = list(vcf_in.header.samples)[0]
    availcols = next(vcf_in.fetch()).format.keys()
    vcf_in.seek(0)

    if 'SS' in availcols:
        cmd = "bcftools query -f'%CHROM %POS %ID %ALT %REF[ %AF %ES %SE %LP %SS]\n' " + gwas_file + "| awk '{print $1, $2, $3, $4, $5, $6, $7, $8, 10^-$9, $10}' | grep -v inf | gzip -c > " + tempout
        #print(cmd)
        subprocess.call(cmd, shell=True)
        print("Done")
        return tempout

    global_fields = [x for x in vcf_in.header.records if x.key == "SAMPLE"][0]
    if 'TotalControls' in global_fields.keys() and 'TotalCases' in global_fields.keys():
        SS = float(global_fields['TotalControls']) + float(global_fields['TotalCases'])
    elif 'TotalControls' in global_fields.keys():
        SS = float(global_fields['TotalControls'])
    else:
        SS = '.'
    cmd = "bcftools query -f'%CHROM %POS %ID %ALT %REF[ %AF %ES %SE %LP]\n' " + gwas_file + "| awk '{print $1, $2, $3, $4, $5, $6, $7, $8, 10^-$9, \"" + str(SS) + "\"}' | grep -v inf | gzip -c > " + tempout
    subprocess.call(cmd, shell=True)
    print("Done")
    return tempout


def file_type(filename):
    s = [x.lower() for x in Path(filename).suffixes]
    if '.vcf' in s:
        return('vcf')
    elif '.gz' in s:
        return('gz')
    else:
        print("Unknown filetype")
        exit()

def check_gwas(gwas_file,gwas_id,index_name):
    print('Checking',gwas_id,gwas_file)
    index_name = index_name.lower()
    #check file exists
    if os.path.exists(gwas_file):
        print('Checking for previously indexed records...')
        #check index exists
        if es.indices.exists(index_name,request_timeout=TIMEOUT):
            #check no data indexed for this gwas
            total = es_gwas_count(gwas_id,index_name)
            print('Number of existing records = '+str(total))
            #removing this check to enable re-indexing of same data
            if int(total)>0:
                return {'error':f'Error: Indexed records exist for index "{index_name}" and gwas_id "{gwas_id}". You will need to delete these first, see README.'}
            else:
                return gwas_file
        else:
            print('No existing index, so no records :)')
            return gwas_file
    else:
        return {'error':'Error: Can not access file '+gwas_file}


def index_gwas_data(gwas_file, gwas_id, index_name, tophits_file):
    print("Indexing gwas data...")

    index_name = index_name.lower()

    tophitsflag = tophits_file is not None
    #set up logging
    formatter=logging.Formatter('%(asctime)s %(msecs)d %(threadName)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s',datefmt='%d-%m-%Y:%H:%M:%S')
    #logging.basicConfig(filename=study_name.replace(':','_')+'.log',level=logging.INFO)
    if not os.path.exists('logs'):
        os.mkdir('logs')
    handler = logging.FileHandler('logs/'+index_name+'-'+gwas_id+'.log')
    handler.setFormatter(formatter)

    logger = logging.getLogger(gwas_id)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    handler2 = logging.FileHandler('logs/indexing-success.log')
    handler2.setFormatter(formatter)

    logger2 = logging.getLogger('success')
    logger2.setLevel(logging.INFO)
    logger2.addHandler(handler2)

    #do some checks
    check_result = check_gwas(gwas_file,gwas_id,index_name)
    if 'error' in check_result:
        print(check_result['error'])
        exit()

    # If vcf then extract to txt.gz
    vcfflag = file_type(gwas_file) == "vcf"
    if vcfflag:
        gwas_file = extract_vcf(gwas_file,gwas_id)

    #remove index from gwas_id
    if '-' in gwas_id:
        gwas_id = gwas_id.split('-')[2]

    #print gwas_data,index_name
    if es.indices.exists(index_name,request_timeout=TIMEOUT):
        print("Index already exists, adding to that one then :)")
    else:
        create_index(index_name)

    #set index to read_write in case it has been set to read only
    read_write_index(index_name)
    
    if tophitsflag:
        tophits = [x.strip() for x in open(tophits_file, 'rt')]
        index_name_tophits = index_name+"-tophits"
        print("Found " + str(len(tophits)) + " tophits")
        if es.indices.exists(index_name_tophits,request_timeout=TIMEOUT):
            print("Index already exists, adding to that one then :)")
        else:
            create_index(index_name_tophits)
        #set index to read_write in case it has been set to read only
        read_write_index(index_name)
    else:
        print("No tophits file specified")
    bulk_data = []
    bulk_data_tophits = []
    counter=0
    start = time.time()
    chunkSize = 100000
    with gzip.open(gwas_file) as f:
        #next(f)
        for line in f:
            if counter % 1000000 == 0:
                end = time.time()
                t=round((end - start), 4)
                print(gwas_file,t,counter)
            if counter % chunkSize == 0:
                try:
                    deque(helpers.streaming_bulk(client=es,actions=bulk_data,chunk_size=chunkSize,request_timeout=TIMEOUT,raise_on_error=True,max_retries=3),maxlen=0)
                except:
                    logger.info('Indexing error: '+gwas_file)
                bulk_data = []
            #print(line.decode('utf-8'))
            l = line.rstrip().decode('utf-8').split(' ')
            l = ['' if x is '.' else x for x in l]
            #print(l)
            #if l[0].startswith('rs'):
            counter+=1
            effect_allele_freq = beta = se = p = n = ''
            try:
                effect_allele_freq = float(l[5])
            except ValueError:
                #print(l)
                logger.info(str(l[2])+' '+str(gwas_id)+' '+gwas_file+' '+str(counter)+' effect_allele_freq error')
            try:
                beta = float(l[6])
            except ValueError:
                logger.info(str(l[2])+' '+gwas_id,gwas_file,counter,'beta error')
            try:
                se = float(l[7])
            except ValueError:
                logger.info(str(l[2])+' '+gwas_id,gwas_file,counter,'se error')
            try:
                p = float(l[8])
            except ValueError:
                logger.info(str(l[2])+' '+gwas_id,gwas_file,counter,'p error')
            try:
                n = int(float(l[9].rstrip()))
            except ValueError:
                logger.info(str(l[2])+' '+gwas_id)
            data_dict = {
                        'chr':l[0],
                        'position':int(l[1]),
                        'gwas_id':gwas_id,
                        'snp_id':l[2],
                        'effect_allele':l[3],
                        'other_allele':l[4],
                        'effect_allele_freq':effect_allele_freq,
                        'beta':beta,
                        'se':se,
                        'p':p,
                        'n':n
            }
            op_dict = {
                    "_index": index_name,
                    #"_id" : gwas_id+':'+l[0]+':'+l[1],
                    #"_op_type":'create',
                    "_type": '_doc',
                    "_source":data_dict
            }
            bulk_data.append(op_dict)
            if tophitsflag:
                if l[2] in tophits:
                    #print(l[0] + " among tophits")
                    #logger.info(l[0] + " among tophits")
                    top_op_dict={
                       '_index':index_name_tophits,
                       "_type": '_doc',
                       "_source":data_dict
                    }
                    bulk_data_tophits.append(top_op_dict)
    #print bulk_data[0]
    #print len(bulk_data)
    try:
        deque(helpers.streaming_bulk(client=es,actions=bulk_data,chunk_size=chunkSize,request_timeout=TIMEOUT,raise_on_error=True,max_retries=3),maxlen=0)
    except:
        logger.info('Indexing error: '+gwas_file)
    #refresh the index
    es.indices.refresh(index=index_name,request_timeout=TIMEOUT)
    total = es_gwas_count(gwas_id,index_name)
    print(f"# Gwas id: {gwas_id}\n# Records in gwas: {counter}\n# Records in index: {total}")
    logger.info('gwas: '+gwas_id+' records in gwas: '+str(counter)+' records in index: '+str(total))
    if counter == int(total):
         print('All records indexed ok')
         logger2.info(index_name+':'+gwas_id+' ok'+' '+str(total))
    else:
         print('Error!, records indexed and records in file not the same')
         logger2.info(index_name+':'+gwas_id+' '+str(counter-int(total))+' missing')

    if tophitsflag:
        deque(helpers.streaming_bulk(client=es,actions=bulk_data_tophits,chunk_size=chunkSize,request_timeout=TIMEOUT,raise_on_error=True,max_retries=3),maxlen=0)
        es.indices.refresh(index=index_name_tophits,request_timeout=TIMEOUT)
        total = es_gwas_count(gwas_id,index_name_tophits)
        print(f"# Records in tophits file: {len(bulk_data_tophits)}\n# Records in tophits index: {total}")
        if len(bulk_data_tophits) == int(total):
            print(f'All tophit records indexed ok')
            logger2.info(index_name+'-tophits:'+gwas_id+' ok'+' '+str(total))
        else:
            print('Error!, tophits indexed and tophits in file not the same')
            logger2.info(index_name+'-tophits:'+gwas_id+' '+str(len(bulk_data_tophits)-int(total))+' missing')
    if vcfflag:
        print("Removing temporary txt.gz file")
        os.remove(gwas_file)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Add a new GWAS study.')
    parser.add_argument('-m,--method', dest='method', help='(create_index, delete_index, index_data)')
    parser.add_argument('-i,--index_name', dest='index_name', help='the index name')
    parser.add_argument('-g,--gwas_id', dest='gwas_id', help='the GWAS id')
    parser.add_argument('-f,--gwas_file', dest='gwas_file', help='the GWAS file')
    parser.add_argument('-t,--tophits', dest='tophits_file', help='List of rs IDs that constitute top hits')
    parser.add_argument('-e,--ehost', dest='ehost', help='elasticsearch host', default='localhost')
    parser.add_argument('-p,--port', dest='port', help='elasticsearch port', default='9200')

    args = parser.parse_args()
    print(args)


    #elasticsearch
    es = Elasticsearch(
        [{'host': args.ehost,'port': args.port}],
    )


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
                exit()
            if args.index_name == None:
                print('Please provide an index name (-i)')
                exit()
            if args.gwas_file == None:
                print('Please provide a gwas file (.vcf.gz or text file .gz) (-f)')
                exit()
            else:
                index_gwas_data(gwas_file=args.gwas_file, gwas_id=args.gwas_id, index_name = args.index_name, tophits_file = args.tophits_file)
        else:
            print("Not a good method")
