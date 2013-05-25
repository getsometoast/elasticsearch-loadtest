elasticsearch-loadtest
======================

App that was put together in about 5 mins to hit elasticsearch with some load...




Useage------------

-
/host=http://my-host:9200<br>
/index-name=my-index<br>
/type-name=my-type<br>
/max-threads=16
/data-path=Path/to-my-data-file.json
/batch-size=500
/shards=1
/replicas=0
/refresh-interval=-1
/drop-existing=true
/total-documents=10000000