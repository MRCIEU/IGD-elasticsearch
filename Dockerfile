FROM python:3.6-alpine

RUN apk add --no-cache --virtual .build-deps make gcc musl-dev zlib zlib-dev bzip2-dev xz-dev

COPY requirements.txt /
RUN pip install -r requirements.txt

RUN mkdir -p /app

RUN wget -O /bin/watcher.py https://raw.githubusercontent.com/MRCIEU/bgc-upload-orchestrator/master/watcher.py?token=AB1fTMa-e8fsZrhTfgrH2VEnYtpvjtCBks5cgZIdwA%3D%3D && chmod 755 /bin/watcher.py


# Path
ENV PATH /app:$PATH

CMD tail -f /dev/null
