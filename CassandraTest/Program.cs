using Cassandra;
using Kucoin.Net.Clients;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace CassandraTest
{
    public class KLine
    {
        public long timestamp { get; set; }
        public double high { get; set; }
        public double low { get; set; }
        public double close { get; set; }
        public double open { get; set; }
    }
    internal class Program
    {
        public static ISession session = null;
        public static DateTime Epoch = new DateTime(1970, 1, 1);
        static void Main(string[] args)
        {
            var cluster = Cluster.Builder()
                     .AddContactPoints("192.168.35.10")
                     .Build();
            // Connect to the nodes using a keyspace
            session = cluster.Connect("testdb");
            session.UserDefinedTypes.Define(UdtMap.For<KLine>());

            var kclient = new KucoinClient();

            // Execute a query on a connection synchronously
            //var rs = session.Execute("SELECT * FROM kucoindata");
            while (true)
            {
                var kucoinkline = kclient.SpotApi.ExchangeData.GetKlinesAsync("BTC-USDT", Kucoin.Net.Enums.KlineInterval.OneMinute, DateTime.UtcNow.AddMinutes(-1)).Result;
                if (kucoinkline.Success)
                {
                    if (kucoinkline.Data.Count() > 0)
                    {
                        var candledata = kucoinkline.Data.LastOrDefault();
                        var kline = new KLine()
                        {
                            close = Convert.ToDouble(candledata.ClosePrice),
                            high = Convert.ToDouble(candledata.HighPrice),
                            open = Convert.ToDouble(candledata.OpenPrice),
                            low = Convert.ToDouble(candledata.LowPrice),
                            timestamp = Convert.ToInt64((DateTime.UtcNow - Epoch).TotalMilliseconds)
                        };
                        var query = session.Prepare("insert into kucoindata (id,data) values (uuid(),?)");
                        var statement = query.Bind(kline);
                        session.Execute(statement);
                    }
                }
                Thread.Sleep(100);
            }
        }
    }
}
