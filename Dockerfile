FROM python:3.6-alpine

RUN pip install elasticsearch

#ADD add-gwas.py /bin/app

# Path
ENV PATH /bin/app:$PATH
