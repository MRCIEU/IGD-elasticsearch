docker build -t bgc-elasticsearch-image .

docker run -d -it -v /Users/be15516/projects/bgc-elasticsearch/data:/data --name bgc-elasticsearch bgc-elasticsearch-image
