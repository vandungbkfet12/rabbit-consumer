using CsvHelper;
using Microsoft.Extensions.Configuration;
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

        static void Main(string[] args)
        {
            try
            {

            //load configuration
            var builder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json");

            var configuration = builder.Build();
            var hostName = configuration["HostName"];
            var port = int.Parse(configuration["Port"]);
            var userName = configuration["UserName"];
            var password = configuration["Password"];
            var queueName = configuration["QueueName"];
            var csvFileName = configuration["CsvFileName"];

            //Init rabbitmq connection
            var factory = new ConnectionFactory() { HostName = hostName, Port = port, UserName = userName, Password = password };
            var rabbitMqConnection = factory.CreateConnection();
            var rabbitMqChannel = rabbitMqConnection.CreateModel();

            rabbitMqChannel.QueueDeclare(queue: queueName,
                                 durable: false,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            rabbitMqChannel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

            int messageCount = Convert.ToInt16(rabbitMqChannel.MessageCount(queueName));
            Console.WriteLine(" Listening to the queue. This channels has {0} messages on the queue", messageCount);

            var consumer = new EventingBasicConsumer(rabbitMqChannel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.Span;
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine(" Location received: " + message);
                var receivedJob = JsonConvert.DeserializeObject<ReceivedJob>(message);

                //Write CSV
                string path = string.Format("{0}\\{1}.csv", System.AppContext.BaseDirectory, csvFileName);
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
            rabbitMqChannel.BasicConsume(queue: queueName,
                                 autoAck: false,
                                 consumer: consumer);

            Thread.Sleep(1000 * messageCount);
            Console.WriteLine(" Connection closed, no more messages.");
            Console.ReadLine();

            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine("Please re-check rabbitmq connection configuration in appsetting.json!");
                Console.ReadLine();
            }
        }
 
        internal class ReceivedJob
        {
            public int clientId { get; set; }
            public string timestamps { get; set; }
        }

    }
}
