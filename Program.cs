using CsvHelper;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;

namespace RabbitConsumer
{
    class Program
    {
        private static string HOSTNAME = "localhost";
        private static string QUEUENAME = "test";

        static void Main(string[] args)
        {
            try
            {

            //Init rabbitmq connection
            var factory = new ConnectionFactory() { HostName = HOSTNAME };
            var rabbitMqConnection = factory.CreateConnection();
            var rabbitMqChannel = rabbitMqConnection.CreateModel();

            rabbitMqChannel.QueueDeclare(queue: QUEUENAME,
                                 durable: false,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            rabbitMqChannel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

            int messageCount = Convert.ToInt16(rabbitMqChannel.MessageCount(QUEUENAME));
            Console.WriteLine(" Listening to the queue. This channels has {0} messages on the queue", messageCount);

            var consumer = new EventingBasicConsumer(rabbitMqChannel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.Span;
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine(" Location received: " + message);
                var receivedJob = JsonConvert.DeserializeObject<ReceivedJob>(message);

                //Write CSV
                string path = string.Format("{0}\\test.csv", System.AppContext.BaseDirectory);
                var str = string.Format("{0},{1},{2}", receivedJob.clientId, receivedJob.timestamps, DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss")); 
                var csv = new StringBuilder();
                csv.AppendLine(str);
                if (File.Exists(path))
                {
                    File.AppendAllText(path, csv.ToString());
                }
                else
                {
                    File.WriteAllText(path, "Id, SentTimestamp, ReceivedTimestamp");
                    File.WriteAllText(path, csv.ToString());
                }                

                rabbitMqChannel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                Thread.Sleep(1000);
            };
            rabbitMqChannel.BasicConsume(queue: QUEUENAME,
                                 autoAck: false,
                                 consumer: consumer);

            Thread.Sleep(1000 * messageCount);
            Console.WriteLine(" Connection closed, no more messages.");
            Console.ReadLine();

            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
 
        internal class ReceivedJob
        {
            public int clientId { get; set; }
            public string timestamps { get; set; }
        }

    }
}
