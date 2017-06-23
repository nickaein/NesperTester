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
            public int Value { get; set; }
        }

        public class FaultEvent
        {
            public int Id { get; set; }
            public int Value { get; set; }
        }

        static EPServiceProvider engine;

        const int NFaults = 20;
        const int NParamPerFault = 5;

        static Dictionary<int, int> _faultStates = new Dictionary<int, int>();
        const int aggregatorFaultId = 1000;

        static void Main(string[] args)
        {
            engine = EPServiceProviderManager.GetDefaultProvider();

            //engine.EPAdministrator.Configuration.AddEventType("NumberEvent", typeof(NumberEvent));
            engine.EPAdministrator.Configuration.AddEventType("FaultEvent", typeof(FaultEvent));

            //CreateSimpleFaults();
            CreateAggregatorFault();

            for (int i = 1; i <= NFaults; i++)
            {
                _faultStates.Add(i, 0);
            }

            _faultStates.Add(aggregatorFaultId, 0);

            //Task.Factory.StartNew(() => SimulateNumberEvents(), TaskCreationOptions.LongRunning);
            Task.Factory.StartNew(() => EngineFeedbacker(), TaskCreationOptions.LongRunning);

            System.Threading.Thread.Sleep(int.MaxValue);
        }

        static void CreateSimpleFaults()
        {
            const int windowLength = 4;

            for (int faultId = 1; faultId <= NFaults; faultId++)
            {
                StringBuilder sb = new StringBuilder();

                for (int j = 0; j < NParamPerFault; j++)
                {
                    sb.AppendFormat("(AVG(NumberEvent{0}{1}.Value) = 1) OR ", faultId, j);
                }

                var rule = sb.ToString();
                rule = rule.Remove(rule.Length - 4, 4);

                sb.Clear();

                for (int j = 0; j < NParamPerFault; j++)
                {
                    sb.AppendFormat("NumberEvent(Id = {0}{1}).win:time({2} sec) AS NumberEvent{0}{1}, ",
                        faultId, j, windowLength);
                }

                var selectSources = sb.ToString();
                selectSources = selectSources.Remove(selectSources.Length - 2, 2);

                var eplStr = string.Format("SELECT {0} as Id, ({1}) AS BooleanValue FROM {2}", faultId, rule, selectSources);
                var statementName = string.Format("statement{0}", faultId);

                var statement = engine.EPAdministrator.CreateEPL(eplStr, statementName, faultId);
                statement.Start();
                statement.Events += Statement_Events;
            }
        }


        static void CreateAggregatorFault()
        {
            const int windowLength = 4;

            StringBuilder sb = new StringBuilder();

            for (int i = 1; i <= NFaults; i++)
            {
                sb.AppendFormat("(MIN(FaultEvent{0}.Value) = 1) OR ", i);
            }

            var rule = sb.ToString();
            rule = rule.Remove(rule.Length - 4, 4);
            sb.Clear();

            for (int faultId = 1; faultId <= NFaults; faultId++)
            {
                sb.AppendFormat("FaultEvent(Id = {0}).win:time({1} sec) AS FaultEvent{0}, ", faultId, windowLength);
                //sb.AppendFormat("FaultEvent(Id = {0}).std:lastevent() AS FaultEvent{0}, ", faultId, windowLength);
            }
            var selectSources = sb.ToString();
            selectSources = selectSources.Remove(selectSources.Length - 2, 2);

            var eplStr = string.Format("SELECT {0} as Id, {1} AS BooleanValue FROM {2}", aggregatorFaultId, rule, selectSources);

            var statementName = string.Format("statement{0}", aggregatorFaultId);

            var statement = engine.EPAdministrator.CreateEPL(eplStr, statementName, aggregatorFaultId);
            statement.Start();
            statement.Events += Statement_Events;
        }


        private static void SimulateNumberEvents()
        {
            int counter = 0;

            while (true)
            {
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
                                evt.Value = 1;
                            }
                            else
                            {
                                evt.Value = 0;
                            }
                        }
                        else
                        {
                            evt.Value = 0;
                        }

                        engine.EPRuntime.SendEvent(evt);
                    }

                    System.Threading.Thread.Sleep(3000 / NFaults);
                }

                counter++;
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
                    if (_faultStates[faultId] != faultValue)
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
                    engine.EPRuntime.SendEvent(new FaultEvent()
                    {
                        Id = item.Key,
                        Value = item.Value,
                    });

                    System.Threading.Thread.Sleep(1000 / NFaults);
                }
            }
        }
    }
}
