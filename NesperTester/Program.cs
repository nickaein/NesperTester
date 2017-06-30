using com.espertech.esper.client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NesperTester
{
    class Program
    {
        public class NumberEvent
        {
            public int Id { get; set; }
            public int Valuea { get; set; }
        }

        public class InterEvent
        {
            public int Id { get; set; }
            public int RelatedFaultId { get; set; }
            public int Valueb { get; set; }
        }

        static EPServiceProvider engine;

        const int NFaults = 1;
        const int NParamPerFault = 2;

        static Dictionary<int, int> _faultStates = new Dictionary<int, int>();
        const int aggregatorFaultId = 1000;

        static void Main(string[] args)
        {
            engine = EPServiceProviderManager.GetDefaultProvider();

            engine.EPAdministrator.Configuration.AddEventType("NumberEvent", typeof(NumberEvent));
            engine.EPAdministrator.Configuration.AddEventType("InterEvent", typeof(InterEvent));

            CreateSimpleFaults();
            CreateAggregatorFault();

            for (int i = 1; i <= NFaults; i++)
            {
                _faultStates.Add(i, 0);
            }

            _faultStates.Add(aggregatorFaultId, 0);

            Task.Factory.StartNew(() => SimulateNumberEvents(), TaskCreationOptions.LongRunning);
            Task.Factory.StartNew(() => EngineFeedbacker(), TaskCreationOptions.LongRunning);

            System.Threading.Thread.Sleep(int.MaxValue);
        }

        static void CreateSimpleFaults()
        {
            const int windowLength = 2;

            for (int faultId = 1; faultId <= NFaults; faultId++)
            {
                StringBuilder sb = new StringBuilder();

                for (int j = 0; j < NParamPerFault; j++)
                {
                    var epl = string.Format(
                        "INSERT INTO InterEvent " +
                        "SELECT {0} as RelatedFaultId, {1} as Id, MIN(Valuea) AS Valueb " +
                    "FROM NumberEvent(Id = {0}{1}).win:time({2} sec)", faultId, j, windowLength);
                    //"FROM NumberEvent(Id = {0}{1}).std:lastevent()", faultId, j, windowLength);

                    var statement = engine.EPAdministrator.CreateEPL(epl);
                    statement.Start();
                }

                {
                    var epl = string.Format(
                        "SELECT {0} AS Id, SUM(Valueb) > 0 AS Boolean " +
                    "FROM InterEvent(RelatedFaultId={0}).std:unique(Id) output every 1 sec", faultId);
                    //"FROM InterEvent(RelatedFaultId={0}).win:time(1 sec)", faultId);

                    var statementName = string.Format("statement{0}", faultId);
                    var statement = engine.EPAdministrator.CreateEPL(epl, statementName, faultId);
                    statement.Start();
                    statement.Events += Statement_Events;

                    epl = string.Format(
                        "INSERT INTO InterEvent " +
                        "SELECT {0} as RelatedFaultId, {1} AS Id, SUM(Valueb) AS Valueb " +
                        //"FROM InterEvent(RelatedFaultId={1}).std:unique(Id)", aggregatorFaultId, faultId);
                        "FROM InterEvent(RelatedFaultId={1}).win:time(1 sec)", aggregatorFaultId, faultId);
                    statement = engine.EPAdministrator.CreateEPL(epl);
                    statement.Start();
                }
            }
        }


        static void CreateAggregatorFault()
        {
            var epl = string.Format("SELECT {0} AS Id, SUM(Valueb) > 0 AS Boolean" +
            //" FROM InterEvent(RelatedFaultId={0}).std:unique(Id)", aggregatorFaultId);
            " FROM InterEvent(RelatedFaultId={0}).win:time(1 sec)", aggregatorFaultId);

            //var statementName = string.Format("statement{0}", aggregatorFaultId);
            //var statement = engine.EPAdministrator.CreateEPL(epl, statementName, aggregatorFaultId);
            //statement.Start();
            //statement.Events += Statement_Events;
        }


        private static void SimulateNumberEvents()
        {
            int counter = 0;

            while (true)
            {
                counter++;

                for (int i = 1; i <= NFaults; i++)
                {
                    for (int j = 0; j < NParamPerFault; j++)
                    {
                        var evt = new NumberEvent();
                        evt.Id = i * 10 + j;

                        if (counter % 2 == 0)
                        {
                            if (i == 1 && j == 0)
                            {
                                evt.Valuea = 1;
                            }
                            else
                            {
                                evt.Valuea = 0;
                            }
                        }
                        else
                        {
                            evt.Valuea = 0;
                        }

                        engine.EPRuntime.SendEvent(evt);

                        System.Threading.Thread.Sleep(2500 / NFaults / NParamPerFault);
                    }
                }
            }
        }

        public static void Statement_Events(object sender, UpdateEventArgs e)
        {
            var underlying = e.NewEvents[0].Underlying as Dictionary<string, object>;

            int faultId = (int)underlying.First().Value;

            if (underlying.Last().Value == null)
            {
                if (_faultStates[faultId] != -1)
                {
                    _faultStates[faultId] = -1;
                    Console.WriteLine("Id {0}: No data", faultId);
                }
            }
            else
            {
                int faultValue = (bool)underlying.Last().Value ? 1 : 0;

                lock (_faultStates)
                {
                    //if (_faultStates[faultId] != faultValue)
                    {
                        _faultStates[faultId] = faultValue;

                        Console.WriteLine("FaultId {0} new value: {1}", faultId, faultValue);
                    }
                }
            }
        }

        private static void EngineFeedbacker()
        {
            while (true)
            {
                Dictionary<int, int> faultStates;
                lock (_faultStates)
                {
                    faultStates = new Dictionary<int, int>(_faultStates);
                }

                foreach (var item in faultStates)
                {
                    if (item.Value != -1)
                    {
                        //engine.EPRuntime.SendEvent(new InterEvent()
                        //{
                        //    RelatedFaultId = aggregatorFaultId,
                        //    Id = item.Key,
                        //    Valueb = item.Value > 0,
                        //});
                    }

                    System.Threading.Thread.Sleep(1000 / NFaults);
                }
            }
        }
    }
}


