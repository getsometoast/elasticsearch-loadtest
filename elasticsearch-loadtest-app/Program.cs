using System;
using System.Collections.Generic;

namespace elasticsearch_loadtest_app
{
	class Program
	{
		private static string _maxThreads;
		private static string _indexName;
		private static string _host;
		private static string _dataPath;
		private static string _batchSize;
		private static string _shards;
		private static string _replicas;
        private static int _totalDocuments;
        private static string _type;

		static void Main(string[] args)
		{
			SetDefaultParameters();

			if (args.Length > 0)
				SetUserDefiniedParameters(args);

            var elasticsearchLoadTester = new ElasticSearchLoadTester(_host, _indexName, int.Parse(_maxThreads), _dataPath,
                                                                      int.Parse(_batchSize), _shards, _replicas, _totalDocuments, _customMapping, _type);

            var key = ConsoleKey.Y;
            while(key == ConsoleKey.Y){
                Console.WriteLine("==================================================");
                Console.WriteLine("Running load test...");
			    elasticsearchLoadTester.RunTest();
                Console.WriteLine("Run complete.");
                Console.WriteLine("Inserted: {0} documents", _totalDocuments);
                Console.WriteLine("Time taken: {0}", elasticsearchLoadTester.TimeTaken.ToString());
                Console.WriteLine("==================================================");
                Console.WriteLine();
                Console.WriteLine("Would you like to run the load test again? y/n");
                key = Console.ReadKey().Key;
                Console.WriteLine();
            }
            Console.WriteLine("Press any key to exit");
            Console.ReadKey();
		}

		private static void SetDefaultParameters()
		{
            _maxThreads = "16";
            _indexName = "myindex";
            _host = "http://localhost:9200";
			_dataPath = "Data/simple-example.json";
			_batchSize = "500";
			_shards = "1";
			_replicas = "0";
            _type = "mytype";
            _totalDocuments = 1000000;
            _customMapping = string.Empty;
		}

		private static void SetUserDefiniedParameters(string[] args)
		{
			var arguments = new Dictionary<string, string>();
			
			foreach (var arg in args)
			{
				var argValArray = arg.Split('=');
				arguments.Add(argValArray[0], argValArray[1]);
			}

			foreach (var argument in arguments)
			{
				switch (argument.Key)
				{
					case "/host":
						_host = argument.Value;
						break;
					case "/index-name":
						_indexName = argument.Value;
						break;
					case "/max-threads":
						_maxThreads = argument.Value;
						break;
					case "/data-path":
						_dataPath = argument.Value;
						break;
					case "/batch-size":
						_batchSize = argument.Value;
						break;
					case "/shards":
						_shards = argument.Value;
						break;
					case "/replicas":
						_replicas = argument.Value;
						break;
                    case "/total-documents":
                        _totalDocuments = int.Parse(argument.Value);
                        break;
                    case "/custom-mapping":
                        _customMapping = argument.Value;
                        break;
                    case "/type":
                        _type = argument.Value;
                        break;
					default:
						break;
				}
			}
		}

        public static string _customMapping { get; set; }
    }
}
