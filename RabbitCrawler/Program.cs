using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitCrawler
{
    class Program
    {

        static ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost", UserName = "guest", Password = "guest" };
        private static IConnection connection = factory.CreateConnection();
        private static IModel channel = connection.CreateModel();

        static void Main(string[] args)
        {
            while (true)
            {
                Console.WriteLine("Input directory to crawl or write 'exit': ");
                string inputDirectory = Console.ReadLine();

                if (inputDirectory.ToLower() == "exit")
                    break;

                SetSearchDirectory(inputDirectory);
            }

            Console.ReadLine();
        }

        static async void SetSearchDirectory(string inputDirectory)
        {
            if (!string.IsNullOrEmpty(inputDirectory))
            {
                DirectoryInfo input = new DirectoryInfo(inputDirectory);
                await Task.Run(() => WalkDirectoryTree(input));
            }
            else
            {
                string[] drives = Environment.GetLogicalDrives();

                foreach (string dir in drives)
                {
                    DriveInfo di = new DriveInfo(dir);

                    if (!di.IsReady)
                    {
                        Console.WriteLine("The drive {0} could not be read", di.Name);
                        continue;
                    }
                    DirectoryInfo rootDir = di.RootDirectory;
                    Console.WriteLine("Current rootDir = " + rootDir);

                    await Task.Run(() => WalkDirectoryTree(rootDir));
                }
            }
        }

        static void WalkDirectoryTree(DirectoryInfo root)
        {
            Console.WriteLine("Crawl Started on " + root);
            FileInfo[] files = null;
            DirectoryInfo[] subDirs = null;

            try
            {
                files = root.GetFiles("*.*");
            }

            catch (UnauthorizedAccessException e)
            {
                channel.ExchangeDeclare(exchange: "logs", type: "fanout");

                var message = e.Message;
                var body = Encoding.UTF8.GetBytes(message);
                channel.BasicPublish(exchange: "logs", routingKey: "", basicProperties: null, body: body);
                Console.WriteLine(" [x] Sent {0}", message);


                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }

            catch (DirectoryNotFoundException e)
            {
                channel.ExchangeDeclare(exchange: "logs", type: "fanout");

                var message = e.Message;
                var body = Encoding.UTF8.GetBytes(message);
                channel.BasicPublish(exchange: "logs", routingKey: "", basicProperties: null, body: body);
                Console.WriteLine(" [x] Sent {0}", message);

                Console.ReadLine();
            }
            if (files != null)
            {
                channel.QueueDeclare(queue: "task_queue", durable: true, exclusive: false, autoDelete: false, arguments: null);
                channel.ExchangeDeclare(exchange: "logs", type: "fanout");

                foreach (FileInfo fi in files)
                {
                    var message = @fi.FullName;
                    var body = Encoding.UTF8.GetBytes(message);


                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true;

                    channel.BasicPublish(exchange: "", routingKey: "task_queue", basicProperties: properties, body: body);

                    string logMessage = "File path: " + message + " sent to 'task_queue'";
                    var logMsg = Encoding.UTF8.GetBytes(logMessage);
                    channel.BasicPublish(exchange: "logs", routingKey: "", basicProperties: null, body: logMsg);

                    Console.WriteLine(" [x] Sent {0}", message);
                }

                subDirs = root.GetDirectories();
                foreach (DirectoryInfo dirInfo in subDirs)
                {
                    WalkDirectoryTree(dirInfo);
                }
            }
        }
    }
}