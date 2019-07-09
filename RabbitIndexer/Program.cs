using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitIndexer
{
    class Program
    {
        static ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost", UserName = "guest", Password = "guest" };
        private static IConnection connection = factory.CreateConnection();
        private static IModel channel = connection.CreateModel();
        private static string queueName;
        static void Main(string[] args)
        {
            //Setting up queue for this indexer
            queueName = channel.QueueDeclare().QueueName;
            byte[] routingKey = Encoding.UTF8.GetBytes(queueName);
            channel.BasicPublish(exchange: "", routingKey: "routing", basicProperties: null, body: routingKey);

            SetRabbitMQ();
        }

        public static async void ReadFromTextFile(string path)
        {
            await Task.Run(() => SearchForWords(path));
        }

        static void SearchForWords(string path)
        {
            string text = null;
            try
            {
                text = File.ReadAllText(path);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            if (!string.IsNullOrEmpty(text))
            {
                Console.WriteLine("Add " + path + " file to database");
                Regex wordRegex = new Regex(@"\w+");
                MatchCollection matches = wordRegex.Matches(text);
                foreach (var word in matches)
                {
                    Console.WriteLine("Add " + word + " to database");
                }
            }
        }


        static void SetRabbitMQ()
        {
            //Setting up queues
            channel.QueueDeclare(queue: "task_queue", durable: true, exclusive: false, autoDelete: false, arguments: null);
            channel.ExchangeDeclare(exchange: "logs", type: "fanout");

            channel.BasicQos(prefetchSize: 0, prefetchCount: 10, global: false);

            Console.WriteLine(" [*] Waiting for messages.");

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body;
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine(" [x] Received {0}", message);

                ReadFromTextFile(message);

                Console.WriteLine(" [x] Done with " + message);

                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

                string msg = message + " Successfully indexed.";
                byte[] logMsg = Encoding.UTF8.GetBytes(msg);
                channel.BasicPublish(exchange: "logs", routingKey: "", basicProperties: null, body: logMsg);
            };
            channel.BasicConsume(queue: "task_queue", autoAck: false, consumer: consumer);

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
