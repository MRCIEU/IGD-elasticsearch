FROM python:3.6-alpine

COPY requirements.txt /
RUN pip install -r requirements.txt

ADD add-gwas.py /bin/app

# Path
ENV PATH /bin/app:$PATH
