elasticsearch-loadtest
======================

App that was put together in about 5 mins to hit elasticsearch with some load...




<h2>Useage</h2>
/host=http://my-host:9200<br>
/index-name=my-index<br>
/type-name=my-type<br>
/max-threads=16<br>
/data-path=Path/to-my-data-file.json<br>
/batch-size=500<br>
/shards=1<br>
/replicas=0<br>
/refresh-interval=-1<br>
/drop-existing=true<br>
/total-documents=10000000<br>