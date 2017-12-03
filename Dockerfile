FROM debian:stretch

# FFIEC Times are in Eastern Time
ENV TZ=EST
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

RUN apt-get -y update && \
    apt-get install -y python3 python3-pip python3-setuptools \
                       libxml2-dev libxslt1-dev --no-install-recommends && \
    apt-get clean

ADD . /app
WORKDIR /app
RUN pip3 install .

ENTRYPOINT ["/usr/local/bin/entrypoint"]
