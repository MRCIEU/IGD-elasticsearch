docker build -t bgc-elasticsearch-image .

#to run in production
docker run -d -it -v /Users/be15516/projects/bgc-elasticsearch/data:/data --name bgc-elasticsearch bgc-elasticsearch-image

#to mount local directory inside container
docker run -d -it -v /Users/be15516/projects/bgc-elasticsearch/data:/data -v "$PWD":/bin/app --name bgc-elasticsearch bgc-elasticsearch-image
