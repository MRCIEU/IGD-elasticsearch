# Upload data to elastic

Example run. Either need to be on elastic server or have tunnel to server open.

Get some data

```
wget -O data.txt.gz https://www.dropbox.com/s/893esdanl3mkd0c/data.txt.gz?dl=0
```

Setup environment

```
virtualenv venv
. venv/bin/activate
pip install -r requirements.txt
```

Create index

```
./add-gwas.py -m create_index -i testing_index
```

Add GWAS

```
./add-gwas.py -m index_data -f data.txt.gz -g 2 -i testing_index
```

Delete index

```
./add-gwas.py -m delete_index -i testing_index
```


### Build image
```
docker build -t bgc-elasticsearch-image .
```

### Create container
```
#to run in production
docker run -d -it -v /path/to/data:/data --name bgc-elasticsearch bgc-elasticsearch-image

#to mount local directory inside container
docker run -d -it -v /path/to/data:/data -v "$PWD":/bin/app --name bgc-elasticsearch bgc-elasticsearch-image
```

### Create index
```
-i = index name
docker exec bgc-elasticsearch add-gwas.py -m create_index -i bgctest
```

### Index data

```
-f = gwas file
-i = index name
-g = gwas id
docker exec bgc-elasticsearch add-gwas.py -m index_data -f /data/data-test.gz -i bgctest -g 1
```
