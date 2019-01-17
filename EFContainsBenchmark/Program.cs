using EFContainsBenchmark.Model;
using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;
using Npgsql.Bulk;
using System.Diagnostics;
using EntityFrameworkCore.MemoryJoin;
using Microsoft.Extensions.Logging;
using System.Linq.Expressions;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Reflection;
using System.Data.Common;
using Newtonsoft.Json;

namespace EFContainsBenchmark
{
    public class Program
    {
        static int MaxTestData;
        static List<TestData> TestData;
        static string[] CaseNames = new[] { "Naive", "Naive (parallel)", "Contains", "Predicate Builder", "Shared Table", "MemoryJoin", "Stored Proc" };
        static TimeSpan MaxExpectedTime = TimeSpan.FromMinutes(10);

        static BenchmarkContext CreateContextAndSeed()
        {
            var ctx = CreateContext();

            if (!ctx.Prices.Any())
            {
                ctx.Database.SetCommandTimeout(TimeSpan.FromHours(1));
                Console.WriteLine("Seeding.. This will take some TIME! Better not to interrupt this process..");

                var securitesCount = 4_000;
                var priceSources = new List<PriceSource>()
                {
                    new PriceSource() { PriceSourceId = 1, Name = "Default" },
                    new PriceSource() { PriceSourceId = 2, Name = "ThirdParty" },
                    new PriceSource() { PriceSourceId = 3, Name = "User" },
                    new PriceSource() { PriceSourceId = 4, Name = "Counterparty" }
                };

                var securities = new List<Security>();
                for (var i = 1; i <= securitesCount; i++)
                {
                    securities.Add(new Security() { SecurityId = i, Ticker = $"Ticker_{i}", Description = $"Security of ticker_{i}" });
                }

                var uploader = new NpgsqlBulkUploader(ctx);
                uploader.Insert(priceSources);
                uploader.Insert(securities);

                var rnd = new Random();
                var prices = new List<Price>();
                var tradedOn = new DateTime(2018, 01, 01);
                var tradedOnMin = new DateTime(1988, 01, 01);

                for (var i = 1; i <= 10_000_000; i++)
                {
                    prices.Add(new Price()
                    {
                        PriceId = i,
                        PriceSourceId = (i % priceSources.Count) + 1,
                        TradedOn = tradedOn,
                        SecurityId = (i % securitesCount) + 1,
                        ClosePrice = 100 + rnd.Next(20),
                        OpenPrice = 100 + rnd.Next(20)
                    });

                    tradedOn = tradedOn.AddDays(-1);
                    if (tradedOn < tradedOnMin)
                        tradedOn = new DateTime(2018, 01, 01);

                    if (prices.Count % 10_000 == 0)
                    {
                        uploader.Insert(prices);
                        prices.Clear();
                    }
                }

                uploader.Insert(prices);
                prices.Clear();

                Console.WriteLine("Seeding is finished..");
            }

            return ctx;
        }

        static BenchmarkContext CreateContext()
        {
            var optionsBuilder = new DbContextOptionsBuilder<BenchmarkContext>();
            optionsBuilder.UseNpgsql("server=localhost;user id=postgres;password=qwerty;database=ef_benchmark");

            var lf = new LoggerFactory();
            lf.AddProvider(new MyLoggerProvider());
            //optionsBuilder.UseLoggerFactory(lf);

            var ctx = new BenchmarkContext(optionsBuilder.Options);
            ctx.Database.EnsureCreated();
            ctx.Database.SetCommandTimeout(TimeSpan.FromMinutes(10));
            return ctx;
        }

        public static void TestPhaseNaive()
        {
            var result = new List<Price>();
            using (var context = CreateContext())
            {
                foreach (var testElement in TestData)
                {
                    result.AddRange(context.Prices.Where(
                        x => x.Security.Ticker == testElement.Ticker &&
                            x.TradedOn == testElement.TradedOn &&
                            x.PriceSourceId == testElement.PriceSourceId));
                }
            }

            result.Clear();
        }

        public static void TestPhaseNaiveParallel()
        {
            var result = new ConcurrentBag<Price>();

            var partitioner = Partitioner.Create(0, TestData.Count);
            Parallel.ForEach(partitioner, range =>
            {
                var subList = TestData.Skip(range.Item1).Take(range.Item2 - range.Item1).ToList();
                using (var context = CreateContext())
                {
                    foreach (var testElement in subList)
                    {
                        var query = context.Prices.Where(
                        x => x.Security.Ticker == testElement.Ticker &&
                            x.TradedOn == testElement.TradedOn &&
                            x.PriceSourceId == testElement.PriceSourceId);
                        foreach (var el in query)
                        {
                            result.Add(el);
                        }
                    }
                }
            });


            result.Clear();
        }

        public static void TestPhase3Contains()
        {
            var result = new List<Price>();
            using (var context = CreateContext())
            {
                var tickers = TestData.Select(x => x.Ticker).Distinct().ToList();
                var dates = TestData.Select(x => x.TradedOn).Distinct().ToList();
                var ps = TestData.Select(x => x.PriceSourceId).Distinct().ToList();

                var data = context.Prices.Where(
                        x => tickers.Contains(x.Security.Ticker) &&
                            dates.Contains(x.TradedOn) &&
                            ps.Contains(x.PriceSourceId))
                    .Select(x => new { Price = x, Ticker = x.Security.Ticker, x.PriceSourceId })
                    .ToList();
                Console.WriteLine($"Read {data.Count} elements");
                var lookup = data.ToLookup(x => $"{x.Ticker}, {x.Price.TradedOn}, {x.PriceSourceId}");

                foreach (var testElement in TestData)
                {
                    var key = $"{testElement.Ticker}, {testElement.TradedOn}, {testElement.PriceSourceId}";
                    result.AddRange(lookup[key].Select(x => x.Price));
                }
            }

            result.Clear();
        }

        public static void TestPhaseBuilder()
        {
            var result = new List<Price>();
            using (var context = CreateContext())
            {

                var baseQuery = from p in context.Prices
                                join s in context.Securities on p.SecurityId equals s.SecurityId
                                select new TestData()
                                {
                                    Ticker = s.Ticker,
                                    TradedOn = p.TradedOn,
                                    PriceSourceId = p.PriceSourceId,
                                    PriceObject = p
                                };


                var tradedOnProperty = typeof(TestData).GetProperty("TradedOn");
                var priceSourceIdProperty = typeof(TestData).GetProperty("PriceSourceId");
                var tickerProperty = typeof(TestData).GetProperty("Ticker");

                var paramExpression = Expression.Parameter(typeof(TestData));
                Expression wholeClause = null;
                foreach (var td in TestData)
                {
                    var elementClause = Expression.AndAlso(
                        Expression.Equal(Expression.MakeMemberAccess(paramExpression, tradedOnProperty), Expression.Constant(td.TradedOn)),
                        Expression.AndAlso(
                            Expression.Equal(Expression.MakeMemberAccess(paramExpression, priceSourceIdProperty), Expression.Constant(td.PriceSourceId)),
                            Expression.Equal(Expression.MakeMemberAccess(paramExpression, tickerProperty), Expression.Constant(td.Ticker)))
                        );

                    if (wholeClause == null)
                        wholeClause = elementClause;
                    else
                        wholeClause = Expression.OrElse(wholeClause, elementClause);
                }

                var query = baseQuery.Where((Expression<Func<TestData, bool>>)Expression.Lambda(wholeClause, paramExpression)).Select(x => x.PriceObject);
                result.AddRange(query);
            }

            result.Clear();
        }

        public static void TestPhaseSharedQueryModel()
        {
            var result = new List<Price>();
            using (var context = CreateContext())
            {
                context.Database.BeginTransaction();

                var reducedData = TestData.Select(x => new SharedQueryModel()
                {
                    PriceSourceId = x.PriceSourceId,
                    Ticker = x.Ticker,
                    TradedOn = x.TradedOn
                }).ToList();
                context.QueryDataShared.AddRange(reducedData);
                context.SaveChanges();

                var query = from p in context.Prices
                            join s in context.Securities on p.SecurityId equals s.SecurityId
                            join t in context.QueryDataShared on new { s.Ticker, p.TradedOn, p.PriceSourceId } equals
                                new { t.Ticker, t.TradedOn, t.PriceSourceId }
                            select p;
                result.AddRange(query);

                context.Database.RollbackTransaction();
            }


            result.Clear();
        }

        public static void TestPhaseMemJoin()
        {
            var result = new List<Price>();
            using (var context = CreateContext())
            {
                var reducedData = TestData.Select(x => new { x.Ticker, TradedOn = x.TradedOn, x.PriceSourceId }).ToList();
                var queryable = context.FromLocalList(reducedData, ValuesInjectionMethod.ViaSqlQueryBody);
                var query = from p in context.Prices
                            join s in context.Securities on p.SecurityId equals s.SecurityId
                            join t in queryable on new { s.Ticker, p.TradedOn, p.PriceSourceId } equals
                                new { t.Ticker, t.TradedOn, t.PriceSourceId }
                            select p;
                result.AddRange(query);
            }


            result.Clear();
        }

        public static void TestStoredProc()
        {
            var result = new List<Price>();
            using (var context = CreateContext())
            {
                var reducedData = TestData.Select(x => new
                {
                    ticker = x.Ticker,
                    traded_on = x.TradedOn,
                    price_source_id = x.PriceSourceId
                }).ToList();
                var json = JsonConvert.SerializeObject(reducedData);

                var cmd = context.Database.GetDbConnection().CreateCommand();
                var p = cmd.CreateParameter();
                p.ParameterName = "@json";
                p.Value = json;
                var data = context.Prices.FromSql(
                    "SELECT * FROM get_prices(@json)",
                    p
                );

                result.AddRange(data);
            }


            result.Clear();
        }

        public static void RunLocalSingleTest(int testNumber, int testDataCount, int runCount)
        {
            MaxTestData = testDataCount;
            var cases = new Action[]
            {
                TestPhaseNaive,
                TestPhaseNaiveParallel,
                TestPhase3Contains,
                TestPhaseBuilder,
                TestPhaseSharedQueryModel,
                TestPhaseMemJoin,
                TestStoredProc
            };

            var testCase = cases[testNumber - 1];

            Expression<Func<Price, TestData>> testDataExpr = x => new TestData()
            {
                Price = x.ClosePrice,
                Description = x.Security.Ticker + " " + x.TradedOn + " " + x.ClosePrice,
                Ticker = x.Security.Ticker,
                PriceSourceId = x.PriceSourceId,
                TradedOn = x.TradedOn
            };

            var context = CreateContextAndSeed();

            TestData = new List<TestData>();
            var pricesCount = context.Prices.Count();
            var batchSize = MaxTestData / 4;
            var testDataQuery = context.Prices.OrderBy(x => x.PriceId);

            TestData.AddRange(testDataQuery.Where(x => x.PriceId >= 0 * pricesCount / 4).Take(batchSize).Select(testDataExpr));
            TestData.AddRange(testDataQuery.Where(x => x.PriceId >= 1 * pricesCount / 4).Take(batchSize).Select(testDataExpr));
            TestData.AddRange(testDataQuery.Where(x => x.PriceId >= 2 * pricesCount / 4).Take(batchSize).Select(testDataExpr));
            TestData.AddRange(testDataQuery.Where(x => x.PriceId >= 3 * pricesCount / 4).Take(batchSize).Select(testDataExpr));

            Stopwatch sw;

            GC.Collect();
            GC.WaitForPendingFinalizers();

            sw = Stopwatch.StartNew();
            for (var i = 0; i < runCount; i++)
            {
                var localSw = Stopwatch.StartNew();
                testCase.Invoke();
                localSw.Stop();

                // if timing for single tes run is 2 times more than MaxExpectedTime then
                //   we can be sure limit for MaxExpectedTime will not be satisfied
                //   for current test case
                if (localSw.Elapsed > MaxExpectedTime * 2)
                    throw new TimeoutException();

            }
            sw.Stop();

            var proc = Process.GetCurrentProcess();

            Console.WriteLine($"{sw.Elapsed.TotalMilliseconds / runCount}\t{proc.WorkingSet64}");
        }

        public static Nullable<TimeSpan> RunRemoteTest(int testNumber, int testDataCount, int runCount)
        {
            var thisLib = Assembly.GetExecutingAssembly().Location;

            var psi = new ProcessStartInfo()
            {
                FileName = "dotnet",
                Arguments = $"\"{thisLib}\" {testNumber} {testDataCount} {runCount}",
                RedirectStandardOutput = true,
                CreateNoWindow = true,
                UseShellExecute = false
            };

            var proc = Process.Start(psi);
            if (!proc.WaitForExit((int)(MaxExpectedTime.TotalMilliseconds * runCount * 1.10)))
            {
                proc.Kill();
            }
            var output = proc.StandardOutput.ReadToEnd();

            var lines = output.Split(new[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);
            var metrics = proc.ExitCode == 0 ? lines.Last().Split('\t') : new[] { "N/A", "N/A", "Process failed!" };

            Console.WriteLine($"{CaseNames[testNumber - 1]}\t{testDataCount}\t{runCount}\t{metrics[0]}\t{metrics[1]}\t{(lines.Length > 2 ? lines[0] : "")}");

            return proc.ExitCode == 0 ? (TimeSpan?)TimeSpan.FromMilliseconds(double.Parse(metrics[0])) : null;
        }

        public static void RunRemoteAllTests()
        {
            var testCases = new[] { 1, 2, 3, 4, 5, 6, 7 };
            var testDataCounts = new[] { 50, 300, 1800, 10800, 64800 };
            var runCounts = new[] { 10 };
            var combinations = from tn in testCases
                               from tdc in testDataCounts
                               from rc in runCounts
                               select new { tn, tdc, rc };

            Console.WriteLine("Case\tData\tRuns\tTime\tMemory\tComment");

            var skipTestCases = new HashSet<int>();

            foreach (var testCase in combinations)
            {
                if (!skipTestCases.Contains(testCase.tn))
                {
                    var timeSpent = RunRemoteTest(testCase.tn, testCase.tdc, testCase.rc);

                    // If timing for current case multiplied 6 (step for next test data count value)
                    //   is longer than 1.5*MaxExpectedTime then we can be sure that next
                    //   step will NOT fit into MaxExpectedTime and we can FAIL and skip next steps
                    //   related to current test case
                    if (!timeSpent.HasValue || timeSpent * 6 > MaxExpectedTime * 1.5)
                    {
                        skipTestCases.Add(testCase.tn);
                    }
                }
                else
                    Console.WriteLine("Skip, because previous test failed....");
            }
        }

        public static void Main(string[] args)
        {
            var context = CreateContextAndSeed();

            if (args.Any())
            {
                var testNumber = int.Parse(args[0]);
                var testDataCount = int.Parse(args[1]);
                var runCount = int.Parse(args[2]);

                RunLocalSingleTest(testNumber, testDataCount, runCount);
            }
            else
            {
                RunRemoteAllTests();
            }
        }
    }
}
