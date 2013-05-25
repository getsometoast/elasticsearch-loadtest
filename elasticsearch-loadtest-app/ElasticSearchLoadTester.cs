using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

namespace elasticsearch_loadtest_app
{
	internal class ElasticSearchLoadTester
	{
		private readonly string _host;
		private readonly string _indexName;
		private readonly string _typeName;
		private readonly int _maxThreads;
		private readonly string _dataPath;
		private readonly int _batchSize;
		private readonly string _shards;
		private readonly string _replicas;
		private readonly string _refreshInterval;
        private readonly bool _dropExistingIndex;
        private readonly int _totalDocuments;

		public ElasticSearchLoadTester(string host, string indexName, string typeName, int maxThreads, string dataPath, int batchSize, string shards, string replicas, string refreshInterval, bool dropExistingIndex, int totalDocuments)
		{
			_host = host;
			_indexName = indexName;
			_typeName = typeName;
			_maxThreads = maxThreads;
			_dataPath = dataPath;
			_batchSize = batchSize;
			_shards = shards;
			_replicas = replicas;
			_refreshInterval = refreshInterval;
            _totalDocuments = totalDocuments;
            _dropExistingIndex = dropExistingIndex;
		}

		public TimeSpan TimeTaken { get; private set; }

		public void RunTest()
		{
			var data = BuildTestData();

            if (_dropExistingIndex)
                DropExistingIndex();

			SetupElasticsearchIndex();

			HitElasticsearchWithSomeLoad(data);
		}

        private void DropExistingIndex()
        {
            var deleteTemplate = File.ReadAllText(ConfigurationManager.AppSettings["Index.Template.Delete"]);
            
            var data = string.Format(deleteTemplate, _indexName);
            var uri = string.Format("{0}/{1}/", _host, _indexName);

            // TODO - do an -XDELETE to the uri
        }

		private void SetupElasticsearchIndex()
		{
            //TODO - this needs to be a put.
            //var settingsTemplate = File.ReadAllText(ConfigurationManager.AppSettings["Index.Template.Settings"]);

            //var data = string.Format(settingsTemplate, _indexName, _shards, _replicas, _refreshInterval);
            //var uri = string.Format("{0}/{1}/_settings", _host, _indexName);

            //using (var webClient = new WebClient())
            //{
            //    webClient.UploadString(uri, data);
            //}
		}

		private void HitElasticsearchWithSomeLoad(string data)
		{
            var uri = string.Format("{0}/{1}/_bulk", _host, _indexName);
            var to = _totalDocuments / _batchSize;

			var options = new ParallelOptions
				{
					MaxDegreeOfParallelism = _maxThreads
				};

			var timer = Stopwatch.StartNew();
			Parallel.For(0, to, options, counter =>
				{
					using (var webClient = new WebClient())
					{
						webClient.UploadString(uri, data);
					}
				});
			timer.Stop();
			TimeTaken = timer.Elapsed;
		}

		private string BuildTestData()
		{
            var bulkHeader = string.Format("{{ \"index\" : {{ \"_index\" : \"{0}\", \"_type\" : \"{1}\" }} }}", _indexName, _typeName);

			var testDocument = File.ReadAllText(_dataPath);
			var testData = new StringBuilder();

			for (int i = 0; i < _batchSize; i ++)
			{
				testData.AppendLine(bulkHeader);
				testData.AppendLine(testDocument);
			}

			return testData.ToString();
		}
	}
}