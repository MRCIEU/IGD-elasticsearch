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
wget -O data.txt.gz https://www.dropbox.com/s/9macshbbkhvfrae/ieu-a-2.txt.gz?dl=0
wget -O data.vcf.gz https://www.dropbox.com/s/3vxqjmocjx6yo2l/ieu-a-2.vcf.gz?dl=0
```

Create a random 'tophits' file

```
gunzip -c data.txt.gz | cut -d " " -f 1 | head -n 80 > tophits.txt
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
./add-gwas.py -m index_data -f data.txt.gz -g 2 -i testing_index
```

Add GWAS using `data.vcf.gz`

```
./add-gwas.py -m index_data -f data.vcf.gz -g 2 -i testing_index
```

Add GWAS and tophits

```
./add-gwas.py -m index_data -f data.vcf.gz -g 2 -i testing_index -t tophits.txt
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

Sometimes an index can be switched to read only, resulting in errros like this:

```
'error': {'type': 'cluster_block_exception', 'reason': 'blocked by: [FORBIDDEN/12/index read-only / allow delete (api)];'},
```

To reset, set the index back to read/write

```
index_name=abc-1
curl -XPUT -H "Content-Type: application/json" http://localhost:9200/$index_name/_settings -d '{"index.blocks.read_only_allow_delete": false}'
```
