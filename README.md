elasticsearch-loadtest
======================

A small app I put together to help me test bulk indexing elasticsearch.

<h2>Useage</h2>

bin\Release\elasticsearch-loadtest-app.exe

/host=http://my-host:9200<br>
/index-name=myindex<br>
/type-name=mytype<br>
/max-threads=16<br>
/data-path=Data/my-data-file.json<br>
/batch-size=500<br>
/shards=1<br>
/replicas=0<br>
/total-documents=10000000<br>
/custom-mapping=Data/custom-mapping.json<br>