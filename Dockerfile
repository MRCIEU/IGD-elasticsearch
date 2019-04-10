FROM python:3.6-alpine

RUN apk add --no-cache --virtual .build-deps make gcc musl-dev zlib zlib-dev bzip2-dev xz-dev curl

COPY requirements.txt /
RUN pip install -r requirements.txt

RUN mkdir -p /app


RUN curl -SL https://github.com/samtools/bcftools/releases/download/1.9/bcftools-1.9.tar.bz2 \
| tar -xvj \
&& bcftools-1.9/configure \
&& make -C bcftools-1.9 \
&& mv bcftools-1.9/bcftools /bin/bcftools \
&& chmod 755 /bin/bcftools

#RUN wget -O /bin/watcher.py https://raw.githubusercontent.com/MRCIEU/bgc-upload-orchestrator/master/watcher.py?token=AB1fTMa-e8fsZrhTfgrH2VEnYtpvjtCBks5cgZIdwA%3D%3D \
#&& chmod 755 /bin/watcher.py
RUN mkdir /home/bin
ADD watcher.py /home/bin
RUN chmod 775 /home/bin/watcher.py


# Path
ENV PATH /app:$PATH
WORKDIR /app

CMD tail -f /dev/null
