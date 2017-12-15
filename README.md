# FFIEC ETL System

This is a system for extracting call reports from the Federal Financial Institutions Examination Council's
[Central Data Repository](https://cdr.ffiec.gov). The data is loaded into Hbase.

The ETL process is executed in a docker container. A running container can be thought of as a 'job'. 
Each job is intended to collect the data from a single institution over all collection periods. The
idea is that many jobs can be executed simultaneously on a container scheduling/orchestration system. 
When running a job, specify the institution you'd like data for by passing a valid [rssd identifier](https://www.alacra.com/alacra/outside/lei/info/rssdid.html)
via the RSSD_TARGET environment variable (or --rssd-target flag).

If you'd just like to update the dataset with a single reporting period (say, in the event of a new call report)
then specify the period with PERIOD_TARGET. RSSD_TARGET and PERIOD_TARGET are not mutually exclusive.

If you're feeling patient, you can omit RSSD_TARGET and PERIOD_TARGET.

## To run an ETL job:
You'll need an instance of hbase. The program assumes you're running a thrift server. 
This will get you something to test against, if for some reason you don't have an Hbase cluster laying around.

```docker run --name hbase -h hbase -d -v $PWD/hbase:/data -p 2181:2181 -p 8080:8080 -p 8085:8085 -p 9160:9160 -p 16010:16010 -p 9090:9090 dajobe/hbase```

From the project root, build the project container.

```docker build . --tag=etlffiec:latest```

Ideally you'd have something like Kubernetes scheduling these jobs on hardware and managing the FFIEC_TOKEN.
For local testing just set some variables in your shell. Here are all the parameters, with their defaults.

* FFIEC_USER=
* FFIEC_TOKEN=
* RSSD_TARGET=None
* PERIOD_TARGET=None
* THRIFT_GATEWAY=127.0.0.1
* THRIFT_PORT=9090
* INIT=False
* TRUNCATE_TABLES=False
* MDRM_PATH=./MDRM.csv
* UPDATE_METADATA=False
* LOGGING_LEVEL=WARNING
* LOGGING_FORMAT=JSON


First you'll want to create the tables and load MDRM metadata into the dictionary table by passing

```docker run --name=initffiec --link=hbase etlffiec --init --thrift-gateway=hbase```

Here's how to truncate the data, if that's your thing.

```docker run --name=truncateffiec --link=hbase etlffiec --truncate-tables --thrift-gateway=hbase```

A similar command for refreshing MDRM metadata, perhaps after the data has been updated and you've rebuilt the container (or if you just ran the truncation example above without thinking)

```docker run --name=updateffiecmetadata --link=hbase etlffiec --update-metadata --thrift-gateway=hbase```

After that, you can loop over an array of RSSD identifiers and execute jobs in parallel.

```
for ID in 131034 720858 229342 819172 65513 753641; do
    docker run -d --name=${ID} --link=hbase -eFFIEC_USERNAME=${FFIEC_USERNAME} -eFFIEC_TOKEN=${FFIEC_TOKEN} -eTHRIFT_GATEWAY=hbase -eLOGGING_LEVEL=INFO -eRSSD_TARGET=${ID} etlffiec;
    sleep 3
done;
```

After you've created these containers, it's possible to re-run the operations with ``docker start``

````
docker start 131034 720858 229342 819172 65513 753641
````

If you're feeling ambitious.

```
docker run -d --name=collectffiec --link=hbase -eFFIEC_USERNAME=${FFIEC_USERNAME} -eFFIEC_TOKEN=${FFIEC_TOKEN} -eLOGGING_LEVEL=INFO -eTHRIFT_GATEWAY=hbase etlffiec
```

To clean up all the above examples

```
for NAME in 131034 720858 229342 819172 65513 753641 initffiec updateffiecmetadata collectffiec hbase; do
    docker stop ${NAME} ; docker rm ${NAME};
done;
```
