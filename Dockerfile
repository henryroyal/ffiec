FROM debian:stretch

# FFIEC Times are in Eastern Time
ENV TZ=EST
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

RUN apt-get -y update && \
    apt-get install -y python3 python3-dev python3-setuptools python3-pip python3-wheel cython3 gcc \
                       libxml2-dev libxslt1-dev \
                       unzip \
                       --no-install-recommends && \
    apt-get clean

ADD requirements.txt /app/
RUN pip3 install -r /app/requirements.txt

ADD . /app
WORKDIR /app
RUN pip3 install .

ADD "https://www.federalreserve.gov/apps/mdrm/pdf/MDRM.zip" /app
RUN unzip MDRM.zip

ENTRYPOINT ["/usr/local/bin/entrypoint"]
