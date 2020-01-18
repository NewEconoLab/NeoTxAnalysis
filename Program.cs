using Microsoft.Extensions.Configuration;
using NEL.NNS.lib;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace NeoTxAnalysis
{
    class Program
    {
        static void Main(string[] args)
        {
            if(args != null && args.Length > 0)
            {
                foreach(var s in args)
                {
                    Console.WriteLine("args:" + s);
                }
            }
            initConfig();
            startRun();
            Console.WriteLine("finshed!");
        }

        static void initConfig()
        {
            var config = JObject.Parse(File.ReadAllText("config.json"));
            Const.testnet_mongodbConnStr = config["block_mongodbConnStr_testnet"].ToString();
            Const.testnet_mongoDatabase = config["block_mongodbDatabase_testnet"].ToString();
            Const.mainnet_mongodbConnStr = config["block_mongodbConnStr_testnet"].ToString();
            Const.mainnet_mongoDatabase = config["block_mongodbDatabase_testnet"].ToString();
            Console.WriteLine(Const.testnet_mongodbConnStr);
            Console.WriteLine(Const.testnet_mongoDatabase);
        }

        static void startRun()
        {
            var t1 = Task.Run(startRunTestnet);
            var t2 = Task.Run(startRunMainnet);
            while(true)
            {
                Thread.Sleep(1000 * 60);
                Console.WriteLine("testnet.status:" + t1.Status);
                Console.WriteLine("mainnet.status:" + t2.Status);
            }
        }

        static void startRunTestnet()
        {
            TxTask txTask = new TxTask
            {
                type = "testnet",
                rConn = new DBInfo
                {
                    connStr = Const.testnet_mongodbConnStr,
                    connDB = Const.testnet_mongoDatabase
                },
                lConn = new DBInfo
                {
                    connStr = Const.testnet_mongodbConnStr,
                    connDB = Const.testnet_mongoDatabase
                },
            };
            Console.WriteLine("testnet start processing...");
            while (true)
            {
                try
                {
                    txTask.process();
                    Thread.Sleep(1000 * 2);
                } catch(Exception ex)
                {
                    File.AppendAllText("testnet.error.log", ex.ToString());
                    Thread.Sleep(1000 * 10);
                }
            }
        }
        static void startRunMainnet()
        {
            TxTask txTask = new TxTask
            {
                type = "mainnet",
                rConn = new DBInfo
                {
                    connStr = Const.mainnet_mongodbConnStr,
                    connDB = Const.mainnet_mongoDatabase
                },
                lConn = new DBInfo
                {
                    connStr = Const.mainnet_mongodbConnStr,
                    connDB = Const.mainnet_mongoDatabase
                },
            };
            Console.WriteLine("mainnet start processing...");
            while (true)
            {
                try
                {
                    txTask.process();
                    Thread.Sleep(1000 * 2);
                }
                catch (Exception ex)
                {
                    File.AppendAllText("testnet.error.log", ex.ToString());
                    Thread.Sleep(1000 * 10);
                }
            }
        }
    }

    class Const
    {
        public static string testnet_mongodbConnStr = "";
        public static string testnet_mongoDatabase = "";
        public static string mainnet_mongodbConnStr = "";
        public static string mainnet_mongoDatabase = "";
    }
    class DBInfo {
        public string connStr { get; set; }
        public string connDB { get; set; }
    }
    

    class TxTask
    {
        public string type { get; set; }
        public DBInfo rConn { get; set; }
        public DBInfo lConn { get; set; }
        public MongoDBHelper mh { get; set; } = new MongoDBHelper();
        private string coll { get; set; } = "txdetail";
        private bool startSyncMode { get; set; } = true;



        public void process()
        {
            var rh = getRH();
            var lh = getLH();
            if(lh >= rh)
            {
                log(lh, rh);
                return;
            }

            for(var index=lh+1; index<rh; ++index)
            {
                var findStr = new JObject { { "index", index} }.ToString();
                var queryRes = mh.GetData(rConn.connStr, rConn.connDB, "block", findStr);
                if(queryRes.Count == 0)
                {
                    //
                    if (startSyncMode) continue;
                    updateLH(index);
                    log(index, rh);
                    continue;
                }

                var item = queryRes[0];
                var txs = (JArray)item["tx"];
                var time = long.Parse(item["time"].ToString());
                //processTxs(txs, index, time);
                log(index, rh);
                //updateLH(index);
            }
        }
        private void processTxs(JArray txs, long index, long time)
        {
            foreach(var tx in txs)
            {
                processTxs(tx, index, time);
                //
                var findStr = new JObject { { "txid", tx["txid"] } }.ToString();
                if(startSyncMode 
                    || mh.GetDataCount(lConn.connStr, lConn.connDB, coll, findStr) == 0)
                {
                    mh.PutData(lConn.connStr, lConn.connDB, coll, tx.ToString());
                }
            }
        }
        private void processTxs(JToken tx, long index, long time)
        {
            var vinout = new JArray();
            //
            var vins = (JArray)tx["vin"];
            tx["vinout"] = vin2pout(vins);
            tx["blockindex"] = index;
            tx["blocktime"] = time;
        }
        private JArray vin2pout(JArray vins)
        {
            if (vins.Count == 0) return vins;
            var res = vins.Select(p => vin2pout(p)).ToArray();
            return new JArray { res };
        }
        private JToken vin2pout(JToken vin)
        {
            var txid = vin["txid"].ToString();
            var vout = int.Parse(vin["vout"].ToString());
            var findStr = new JObject { { "txid", txid } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, coll, findStr);
            if (queryRes.Count == 0) throw new Exception("not find tx by txid:" + txid);

            var item = queryRes[0];
            var vouts = (JArray)item["vout"];
            var res = vouts.Where(p => int.Parse(p["n"].ToString()) == vout).First();
            return res;
        }


        private void log(long lh, long rh, string key="txdetail")
        {
            Console.WriteLine("{0} {1}.{2}.processed: {3}/{4}", DateTime.Now.ToString("u"), type, key, lh, rh);
        }
        private void updateLH(long index, string key="txdetail")
        {
            var findStr = new JObject { { "counter", key } }.ToString();
            if(mh.GetDataCount(lConn.connStr, lConn.connDB, "system_counter", findStr) == 0)
            {
                var newdata = new JObject {
                    { "counter", key},
                    { "lastBlockindex", index},
                }.ToString();
                mh.PutData(lConn.connStr, lConn.connDB, "system_counter", newdata);
                return;
            }
            var updateStr = new JObject { { "$set", new JObject {
                { "lastBlockindex", index}
            } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, "system_counter", updateStr, findStr);
        }
        private long getLH(string key="txdetail")
        {
            var findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, "system_counter", findStr);
            if (queryRes.Count == 0) return -1;

            var item = queryRes[0];
            var hh = long.Parse(item["lastBlockindex"].ToString());
            return hh;
        }
        private long getRH(string key="tx")
        {
            var findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(rConn.connStr, rConn.connDB, "system_counter", findStr);
            if (queryRes.Count == 0) return -1;

            var item = queryRes[0];
            var hh = long.Parse(item["lastBlockindex"].ToString());
            return hh;
        }
    }
}
