# Upload data to elastic


## Connection

Either need to have tunnel to server open or run on elasticsearch server and specify the host and port using

```
./add-gwas.py ... -h HOSTNAME -p PORT
```

## Example

Get some data

```
wget -O data.txt.gz https://www.dropbox.com/s/893esdanl3mkd0c/data.txt.gz?dl=0
wget -O data.bcf https://www.dropbox.com/s/5v863r7w6vgpl3d/data.bcf?dl=0
```

Create a random 'tophits' file

```
gunzip -c data.txt.gz | cut -d " " -f 1 | head -n 80 > tophits.txt
```

gunzip -c data.txt.gz 

Setup environment

```
python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt
```

Create index

```
./add-gwas.py -m create_index -i testing_index
```

Add GWAS using `data.txt.gz`

```
./add-gwas.py -m index_data -f data.txt.gz -g 2 -i testing_index
```

Add GWAS using `data.bcf`

```
./add-gwas.py -m index_data -f data.bcf -g 2 -i testing_index
```

Add GWAS and tophits

```
./add-gwas.py -m index_data -f data.bcf -g 2 -i testing_index -t tophits.txt
```


Delete index

```
./add-gwas.py -m delete_index -i testing_index
```


## Running on docker

```
docker-compose up --build -d
```


### Create index
```
# -i = index name
docker exec bgc-elasticsearch add-gwas.py -m create_index -i bgctest
```

### Index data

```
# -f = gwas file
# -i = index name
# -g = gwas id (.txt.gz or .bcf)
# -t = tophits file (one rsid per row only)
docker exec bgc-elasticsearch add-gwas.py -m index_data -f /data/data.bcf -i bgctest -g 1 -t tophits.txt
```
