using System.Text;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Extensions.Configuration;

await MyNamesapce.Program.doSend();

namespace MyNamesapce
{

    static class Program
    {
        // connection string to the Event Hubs namespace
        private const string connectionString = "xxxxxx";
        // name of the event hub
        private const string eventHubName = "xxxxxx";

        // number of events to be sent to the event hub
        private const int numOfEvents = 3;

        static Random sensoridRnd = new Random();

        static Random tempRnd = new Random();

        static Random humidityRnd = new Random();

        static Random statusRnd = new Random();

        public static async Task doSend()
        {
           IConfigurationRoot configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile(path: "appsettings.json")
            .Build();

            int sensorid = 0;
            int.TryParse(configuration["sensor_id"], out sensorid);

            for (; ; )
            {
                // The Event Hubs client types are safe to cache and use as a singleton for the lifetime
                // of the application, which is best practice when events are being published or read regularly.
                await using EventHubProducerClient producerClient = new EventHubProducerClient(connectionString, eventHubName);
                // Create a batch of events 
                using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
                for (int i = 1; i <= numOfEvents; i++)
                {
                   
                    float temperature = tempRnd.NextSingle() * 100;

                    float humidit = humidityRnd.NextSingle() * 100;

                    string status = statusRnd.Next(0, 100) % 2 == 0 ? "OK" : "NG";

                    string senddatetime = DateTime.UtcNow.ToString("s");

                    string payload = $"{{\"id\":{sensorid},\"temperature\":{temperature},\"humidity\":{humidit},\"status\":\"{status}\",\"sentdatetime\":\"{senddatetime}\"}}";

                    Console.WriteLine($"Paylaod is  {payload} .");
                    if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(payload))))
                    {
                        // if it is too large for the batch
                        throw new Exception($"Event {i} is too large for the batch and cannot be sent.");
                    }
                }
                try
                {
                    // Use the producer client to send the batch of events to the event hub
                    await producerClient.SendAsync(eventBatch);
                    Console.WriteLine($"A batch of {numOfEvents} events has been published.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.StackTrace);
                }
                await Task.Delay(500);
            }
        }
    }
}
