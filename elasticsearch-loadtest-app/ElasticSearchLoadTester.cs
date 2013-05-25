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

		public ElasticSearchLoadTester(string host, string indexName, string typeName, int maxThreads, string dataPath, int batchSize, string shards, string replicas, string refreshInterval, bool dropExistingIndex)
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
            var uri = _host + _indexName + "_settings";
        }

		private void SetupElasticsearchIndex()
		{
            var settingsTemplate = File.ReadAllText(ConfigurationManager.AppSettings["Index.Template.Settings"]);

            var data = string.Format(settingsTemplate, _indexName, _shards, _replicas, _refreshInterval);
			var uri = _host + _indexName + "_settings";

			using (var webClient = new WebClient())
			{
				webClient.UploadString(uri, data);
			}
		}

		private void HitElasticsearchWithSomeLoad(string data)
		{
			var uri = _host + _indexName + "_bulk";

			var options = new ParallelOptions
				{
					MaxDegreeOfParallelism = _maxThreads
				};

			var timer = Stopwatch.StartNew();
			Parallel.For(0, 100, options, counter =>
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
			var testDocument = File.ReadAllText(_dataPath);
			var testData = new StringBuilder();

			for (int i = 0; i < _batchSize; i ++)
			{
				testData.AppendLine("");
				testData.AppendLine(testDocument);
			}

			return testData.ToString();
		}
	}
}