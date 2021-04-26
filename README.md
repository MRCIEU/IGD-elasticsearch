# Upload data to elastic


## Connection

Either need to have tunnel to server open or run on elasticsearch server and specify the host and port using

```
./add-gwas.py ... -h HOSTNAME -p PORT
```

## VCF files

You'll need the [bcftools](https://samtools.github.io/bcftools/) binary on your PATH in order to be able to read in `.vcf.gz` files.

## Example

Get some data

```
wget https://gwas.mrcieu.ac.uk/files/eqtl-a-ENSG00000064545/eqtl-a-ENSG00000064545.vcf.gz
wget https://gwas.mrcieu.ac.uk/files/eqtl-a-ENSG00000064545/eqtl-a-ENSG00000064545.vcf.gz.tbi
```

Create a random 'tophits' file

```
zless eqtl-a-ENSG00000064545.vcf.gz| cut -f3 | grep '^rs' | head -n 80 > tophits.txt
```

Setup environment

```
python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt
```

Optionally setup local elasticsearch

```
docker-compose -f docker-compose-es.yml up -d
```

Create index

```
./add-gwas.py -m create_index -i testing_index
```

Add GWAS using `data.txt.gz`

```
./add-gwas.py -m index_data -f eqtl-a-ENSG00000064545.vcf.gz -g 2 -i testing_index
```

Add GWAS using `data.vcf.gz`

```
./add-gwas.py -m index_data -f data.vcf.gz -g 2 -i testing_index
```

Add GWAS and tophits

```
./add-gwas.py -m delete_index  -i testing_index
./add-gwas.py -m index_data -f eqtl-a-ENSG00000064545.vcf.gz -g 2 -i testing_index -t tophits.txt
```

In parallel

```
cat list_of_gwas_ids | parallel -j num_threads ./add_gwas.py -m index_data -f {}/data.vcf.gz -g {} -i testing_index -t {}/tophits.txt
```


Delete index

```
./add-gwas.py -m delete_index -i testing_index
```


## Build docker image

```
docker build -t igd-elasticsearch .
```


### Create index
```
# -i = index name
docker run igd-elasticsearch python add-gwas.py -m create_index -i igdtest
```

### Index data

```
# -f = gwas file
# -i = index name
# -g = gwas id (.txt.gz or .vcf)
# -t = tophits file (one rsid per row only)
docker run igd-elasticsearch add-gwas.py -m index_data -f /data/data.vcf.gz -i igdtest -g 1 -t tophits.txt
```

# Issues

### Cluster is red

Check the nodes

```
curl -X GET "localhost:9200/_cat/nodes?v=true"
```

See if any nodes are not connected and reboot them

### Read only indexes

Sometimes an index can be switched to read only, resulting in errros like this:

```
'error': {'type': 'cluster_block_exception', 'reason': 'blocked by: [FORBIDDEN/12/index read-only / allow delete (api)];'},
```

To reset, set the index back to read/write

```
index_name=abc-1
curl -XPUT -H "Content-Type: application/json" http://localhost:9200/$index_name/_settings -d '{"index.blocks.read_only_allow_delete": false}'
```

### No space on data nodes

Clean up dumps on elastic workers

```
sudo rm /var/lib/elasticsearch/*
```

# Update by query

Updating a large amount of data is slow and potentially problematic due to timeouts.

```
from elasticsearch import Elasticsearch

gwas_id='21'
index='ieu-b-test'

es = Elasticsearch(
        [{'host': 'localhost','port': '9200'}],
)

body={
        "script": {
            "source": "ctx._source.gwas_id='"+update_id+"-deprecated'",
            "lang":"painless"
        },
        "query":{
            "term":{"gwas_id":update_id}
        }
    }
es.update_by_query(index=index,body=body,request_timeout=600,conflicts='abort',slices='auto',wait_for_completion=False)
```

# Delete by query

Takes about 10 minutes for 10 million records

```
from elasticsearch import Elasticsearch

gwas_id='21'
index='ieu-b-test'

es = Elasticsearch(
        [{'host': 'localhost','port': '9200'}],
)

body={
        "query":{
            "term":{"gwas_id":gwas_id}
        }
    }
es.delete_by_query(index=index,body=body,request_timeout=600,conflicts='abort',slices='auto',wait_for_completion=False)
```

# Creating an alias and selecting an index to write to

Check aliases

```
curl -X GET "localhost:9200/_cat/aliases?v" | sort -k1,2
```

To create an alias 

```
curl -XPOST 'http://localhost:9200/_aliases' -H 'Content-Type: application/json' -d '{"actions" : [{ "add" : { "index" : "ebi-a-1", "alias" : "ebi-a"} }]}';
```

To create an alias and select an index for writing

```
curl -XPOST 'http://localhost:9200/_aliases' -H 'Content-Type: application/json' -d '{"actions" : [{ "add" : { "index" : "ebi-a-1", "alias" : "ebi-a" , "is_write_index" : true} }]}';
curl -XPOST 'http://localhost:9200/_aliases' -H 'Content-Type: application/json' -d '{"actions" : [{ "add" : { "index" : "ebi-a-1-tophits", "alias" : "ebi-a-tophits" , "is_write_index" : true} }]}';
```

https://www.elastic.co/guide/en/elasticsearch/reference/6.8/indices-aliases.html#aliases-write-index

Aliases that do not explicitly set is_write_index: true for an index, and only reference one index, will have that referenced index behave as if it is the write index until an additional index is referenced. At that point, there will be no write index and writes will be rejected.
