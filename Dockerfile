FROM python:3.6-alpine

RUN pip install elasticsearch

ADD add-gwas.py /bin

# Path
ENV PATH /bin:$PATH
