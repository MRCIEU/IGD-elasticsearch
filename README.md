### Build image
```
docker build -t bgc-elasticsearch-image .
```

### Create container
```
#to run in production
docker run -d -it -v /Users/be15516/projects/bgc-elasticsearch/data:/data --name bgc-elasticsearch bgc-elasticsearch-image

#to mount local directory inside container
docker run -d -it -v /Users/be15516/projects/bgc-elasticsearch/data:/data -v "$PWD":/bin/app --name bgc-elasticsearch bgc-elasticsearch-image
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
