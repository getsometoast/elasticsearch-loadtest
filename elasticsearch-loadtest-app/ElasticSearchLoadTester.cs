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
        private readonly int _maxThreads;
        private readonly string _dataPath;
        private readonly int _batchSize;
        private readonly string _shards;
        private readonly string _replicas;
        private readonly int _totalDocuments;
        private readonly string _customMapping;
        private readonly string _type;

        public ElasticSearchLoadTester(string host, string indexName, int maxThreads, string dataPath, int batchSize, string shards, string replicas, int totalDocuments, string customMapping, string type)
        {
            _host = host;
            _indexName = indexName;
            _maxThreads = maxThreads;
            _dataPath = dataPath;
            _batchSize = batchSize;
            _shards = shards;
            _replicas = replicas;
            _totalDocuments = totalDocuments;
            _customMapping = customMapping;
            _type = type;
        }

        public TimeSpan TimeTaken { get; private set; }

        public void RunTest()
        {
            var testData = BuildTestData();

            DropExistingIndex();

            SetupIndex();

            SetRefreshInterval("-1");

            if (!string.IsNullOrEmpty(_customMapping))
                SetupDocumentMapping();

            HitElasticsearchWithSomeLoad(testData);

            SetRefreshInterval("1s");
        }

        private void SetRefreshInterval(string interval)
        {
            var uri = string.Format("{0}/{1}/_settings", _host, _indexName);
            var data = string.Format(File.ReadAllText("Data/index-bulk-settings-template.json"), interval);

            using (var webClient = new WebClient())
            {
                webClient.UploadString(uri, "PUT", data);
            }
        }

        private void SetupDocumentMapping()
        {
            var uri = string.Format("{0}/{1}/{2}/_mapping", _host, _indexName, _type);
            var data = File.ReadAllText(_customMapping);

            using (var webClient = new WebClient())
            {
                webClient.UploadString(uri, "PUT", data);
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
                    // index might not exist yet but we don't care as will create it in the next step.
                }
            }
        }

        private void SetupIndex()
        {
            var settingsTemplate = File.ReadAllText(ConfigurationManager.AppSettings["Index.Template.Settings"]);
            var uri = string.Format("{0}/{1}", _host, _indexName);

            var data = string.Format(settingsTemplate, _shards, _replicas);

            using (var webClient = new WebClient())
            {
                webClient.UploadString(uri, "PUT", data);
            }
        }

        private void HitElasticsearchWithSomeLoad(string data)
        {
            var uri = string.Format("{0}/{1}/{2}/_bulk", _host, _indexName, _type);
            var to = (_totalDocuments / _batchSize) == 0 ? 1 : (_totalDocuments / _batchSize);

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
            var bulkHeader = string.Format("{{ \"index\" : {{ \"_index\" : \"{0}\", \"_type\" : \"{1}\" }} }}", _indexName, _type);

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