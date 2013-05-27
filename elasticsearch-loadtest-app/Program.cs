﻿using System.Collections.Generic;
using System.Configuration;
using System;

namespace elasticsearch_loadtest_app
{
	class Program
	{
		private static string _maxThreads;
		private static string _typeName;
		private static string _indexName;
		private static string _host;
		private static string _dataPath;
		private static string _batchSize;
		private static string _shards;
		private static string _replicas;
		private static string _refreshInterval;
        private static bool _dropExistingIndex;
        private static int _totalDocuments;

		// #######################################################
		// 
		// Available Options: 
		//	/host= host address e.g. http://localhost:9200
		//	/index-name= index name e.g. tweet
		//	/type-name= type name e.g. type1
		//	/max-threads= maximum threads for parallel loop
		//	/data-path= file path to a data file to use as the document in the load test
		//	/batch-size= size of a bulk indexing request
		//	/shards= initial number of shards in the index
		//	/replicas= initial number of replicas
		//	/refresh-interval= initial index refresh interval
		// 
		// #######################################################
		static void Main(string[] args)
		{
			SetDefaultParameters();

			if (args.Length > 0)
				SetUserDefiniedParameters(args);

            var elasticsearchLoadTester = new ElasticSearchLoadTester(_host, _indexName, _typeName, int.Parse(_maxThreads), _dataPath,
                                                                      int.Parse(_batchSize), _shards, _replicas, _refreshInterval, _dropExistingIndex, _totalDocuments, _customMapping);

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
			_maxThreads = ConfigurationManager.AppSettings["Default.MaxThreads"];
			_indexName = ConfigurationManager.AppSettings["Default.IndexName"];
			_typeName = ConfigurationManager.AppSettings["Default.TypeName"];
			_host = ConfigurationManager.AppSettings["Default.Host"];
			_dataPath = ConfigurationManager.AppSettings["Default.DataPath"];
			_batchSize = ConfigurationManager.AppSettings["Default.BatchSize"];
			_shards = ConfigurationManager.AppSettings["Default.Shards"];
			_replicas = ConfigurationManager.AppSettings["Default.Replicas"];
			_refreshInterval = ConfigurationManager.AppSettings["Default.RefreshInterval"];
            _dropExistingIndex = false;
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
					case "/type-name":
						_typeName = argument.Value;
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
					case "/refresh-interval":
						_refreshInterval = argument.Value;
						break;
                    case "/drop-existing":
                        _dropExistingIndex = bool.Parse(argument.Value);
                        break;
                    case "/total-documents":
                        _totalDocuments = int.Parse(argument.Value);
                        break;
                    case "/custom-mapping":
                        _customMapping = argument.Value;
                        break;
					default:
						break;
				}
			}
		}

        public static string _customMapping { get; set; }
    }
}
