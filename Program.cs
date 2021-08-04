using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Consumer;

namespace QueryEventHub
{
    class Program
    {
        static void Main(string[] args)
        {
            // --------------------------------------------
            // Command line arguments handler
            // --------------------------------------------
            var cmd = new CommandLineApplication();
            var ehConnString = cmd.Option("-c | --connString <value>", "EventHub Connection String", CommandOptionType.SingleValue);
            var ehName = cmd.Option("-n | --name <value>", "EventHub Name", CommandOptionType.SingleValue);
            var timeOut = cmd.Option("-t | --timeout <value>", "Seconds to wait while reading the EventHub (default & minimum is 5)", CommandOptionType.SingleValue);

            cmd.OnExecute(() =>
            {
                // Error out if connection string not set
                // Connection strings CAN OPTIONALLY inlcude a hub name, so letting that go
                // even if a name is not provided.
                if (ehConnString.Value()!=null)
                {
                    // Default Timeout if not set
                    long iTimeOut = 5; //default to 5 seconds if not specified
                    if (timeOut.Value()!=null)
                        iTimeOut = long.Parse(timeOut.Value()); 
                    
                    // Force to 5 if set to lower than the min of 5
                    if (iTimeOut < 5)
                        iTimeOut = 5;

                    string result = processAllEvents(ehConnString.Value(), ehName.Value(), iTimeOut).GetAwaiter().GetResult();
                    //Console.WriteLine(result); 
                    return 0;
                }
                else
                {
                    Console.WriteLine("Must provide a connection string at a minimum.");
                    cmd.ShowHelp();
                    return 1;
                }
                
            });

            cmd.HelpOption("-? | -h | --help");
            cmd.Execute(args); 



        }

        private static async Task<string> processAllEvents(string ehConnectionString, string ehName, long iTimeOut)
        {
            var consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            var consumer = new EventHubConsumerClient(
                consumerGroup,
                ehConnectionString,
                ehName);

            try
            {
                using CancellationTokenSource cancellationSource = new CancellationTokenSource();
                //Cancel after 5 seconds or 100 events (whichever comes first)
                cancellationSource.CancelAfter(TimeSpan.FromSeconds(iTimeOut));
                int eventsRead = 0;
                int maximumEvents = 100;

                await foreach (PartitionEvent partitionEvent in consumer.ReadEventsAsync(cancellationSource.Token))
                {
                    string readFromPartition = partitionEvent.Partition.PartitionId;         
                    Console.WriteLine("\tReceived event: {0}", Encoding.UTF8.GetString(partitionEvent.Data.Body.ToArray()));

                    //Debug.WriteLine($"Read event of length { eventBodyBytes.Length } from { readFromPartition }");
                    eventsRead++;

                    if (eventsRead >= maximumEvents)
                    {
                        break;
                    }
                }
                return "Processed";
            }
            catch (TaskCanceledException)
            {
                // This is expected if the cancellation token is
                // signaled.
                return "Canceled";
            }
            finally
            {
                await consumer.CloseAsync();
            }

        }
    }
}
