using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting.Channels;
using System.Text;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.Configuration.Client;
using Couchbase.Core;
using System.Diagnostics;

namespace CouchbaseAsyncExample
{
    class Program
    {
        public class Person
        {
            private static readonly string _type = "person";

            public Person()
            {
                Type = _type;
            }

            public string Id { get; set; }

            public string Name { get; set; }

            public int Age { get; set; }

            public string Type { get; private set; }

            public string Address { get { return "Quảng Nam"; } }
        }

        public class Group
        {
            public string GroupId { get; set; }
            public List<string> ListPersonId { get; set; }

            public string Name { get; set; }
        }

        private static void Main(string[] args)
        {
            var count = 100;
            var config = new ClientConfiguration
            {
                Servers = new List<Uri>
                {
                    //change this to your cluster to bootstrap
                    new Uri("http://localhost:8091/pools")
                }
            };

            ClusterHelper.Initialize(config);
            var bucket = ClusterHelper.GetBucket("default");

            //var items = new List<Person>();
            //for (int i = 0; i < count; i++)
            //{
            //    items.Add(new Person {Age = i, Name = "Bảo Address " + i, Id = i.ToString()});
            //    Console.WriteLine("++Person: {0}", i);
            //}

            var groups = new List<Group>();
            for (int i = 0; i < count; i++)
            {
                groups.Add(new Group { ListPersonId = new List<string>(){i.ToString(), (i+1).ToString(), (i+2).ToString()}, 
                    Name = "Group " + i, GroupId = i.ToString() });
                Console.WriteLine("++Group: {0}", i);
            }

            Task.Run(async () => await UpsertAllAsync(groups, bucket)).ConfigureAwait(false);

            Console.Read();
            ClusterHelper.Close();
        }



        public static async Task UpsertAllAsync(List<Person> persons, IBucket bucket)
        {
            var stopwatch = new Stopwatch();

            stopwatch.Start();
            var tasks = new List<Task<OperationResult<Person>>>();
            persons.ForEach(x => bucket.UpsertAsync(x.Id, x));

            var results = await Task.WhenAll(tasks).ConfigureAwait(false);
            Console.WriteLine("Total upserted: {0} in {1} seconds", results.Length, stopwatch.Elapsed.TotalSeconds);
        }

        public static async Task UpsertAllAsync(List<Group> persons, IBucket bucket)
        {
            var stopwatch = new Stopwatch();

            stopwatch.Start();
            var tasks = new List<Task<OperationResult<Person>>>();
            persons.ForEach(x => bucket.UpsertAsync(x.GroupId, x));

            var results = await Task.WhenAll(tasks).ConfigureAwait(false);
            Console.WriteLine("Total Group upserted: {0} in {1} seconds", results.Length, stopwatch.Elapsed.TotalSeconds);
        }
    }
}
