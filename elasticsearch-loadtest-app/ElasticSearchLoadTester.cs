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
        private readonly string _customMapping;

        public ElasticSearchLoadTester(string host, string indexName, string typeName, int maxThreads, string dataPath, int batchSize, string shards, string replicas, string refreshInterval, bool dropExistingIndex, int totalDocuments, string customMapping)
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
            _customMapping = customMapping;
        }

        public TimeSpan TimeTaken { get; private set; }

        public void RunTest()
        {
            var data = BuildTestData();

            // if index exists, delete it
            // setup index with desired parameters
            // setup load test settings (refresh interval, merge frequency)
            // if mapping supplied, setup mapping

            // perform load test

            // reset index refresh rate, merge frequency


            if (_dropExistingIndex)
                DropExistingIndex();

            SetupElasticsearchIndex();

            if (!string.IsNullOrEmpty(_customMapping))
                SetupDocumentMapping();

            HitElasticsearchWithSomeLoad(data);
        }

        private void SetupDocumentMapping()
        {
            var uri = string.Format("{0}/{1}/_mapping", _host, _indexName);
            var data = File.ReadAllText(_customMapping);

            using (var webClient = new WebClient())
            {
                webClient.UploadString(uri, data);
            }
        }

        private void DropExistingIndex()
        {
            var uri = string.Format("{0}/{1}/", _host, _indexName);

            using (var webClient = new WebClient())
            {
                try
                {
                    webClient.UploadString(uri, "DELETE", string.Empty);
                }
                catch (WebException)
                {
                }
            }
        }

        private void SetupElasticsearchIndex()
        {
            var settingsTemplate = File.ReadAllText(ConfigurationManager.AppSettings["Index.Template.Settings"]);
            var uri = string.Empty;
            var shards = string.Empty;

            if (_dropExistingIndex)
            {
                shards = string.Format("\"number_of_shards\" : \"{0}\",", _shards);
                uri = string.Format("{0}/{1}", _host, _indexName);
            }
            else
                uri = string.Format("{0}/{1}/_settings", _host, _indexName);

            var replicas = string.Format("\"number_of_replicas\" : \"{0}\",", _replicas);
            var refresh = string.Format("\"refresh_interval\" : \"{0}\"", _refreshInterval);

            var settingsBody = string.IsNullOrEmpty(shards) ? replicas + refresh : shards + replicas + refresh;
            var data = string.Format(settingsTemplate, settingsBody);

            using (var webClient = new WebClient())
            {
                webClient.UploadString(uri, "PUT", data);
            }
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

            for (int i = 0; i < _batchSize; i++)
            {
                testData.AppendLine(bulkHeader);
                testData.AppendLine(testDocument);
            }

            return testData.ToString();
        }
    }
}