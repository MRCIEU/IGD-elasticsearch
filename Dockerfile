FROM python:3.6-alpine

RUN apk add --no-cache --virtual .build-deps make gcc musl-dev zlib zlib-dev bzip2-dev xz-dev curl

COPY . /app
RUN pip install -r /app/requirements.txt

RUN curl -SL https://github.com/samtools/bcftools/releases/download/1.9/bcftools-1.9.tar.bz2 \
| tar -xvj \
&& bcftools-1.9/configure \
&& make -C bcftools-1.9 \
&& mv bcftools-1.9/bcftools /bin/bcftools \
&& chmod 755 /bin/bcftools

# Path
ENV PATH /app:$PATH
WORKDIR /app

CMD ["python", "add-gwas.py"]
