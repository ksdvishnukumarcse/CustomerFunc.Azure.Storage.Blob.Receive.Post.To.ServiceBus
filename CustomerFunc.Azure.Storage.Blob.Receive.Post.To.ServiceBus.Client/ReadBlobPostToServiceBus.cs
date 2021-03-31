using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace CustomerFunc.Azure.Storage.Blob.Receive.Post.To.ServiceBus.Client
{
    public static class ReadBlobPostToServiceBus
    {
        private static IConfigurationRoot config;
        [FunctionName("read-blob-post-to-service-bus")]
        //[return: ServiceBus("text-file-topic", Connection = "ServiceBusConnectionString")]
        public async static Task Run([BlobTrigger("blob-container/{name}", Connection = "AzureWebJobsStorage")] Stream myBlob, string name, ExecutionContext context, ILogger log)
        {
            config = new ConfigurationBuilder().SetBasePath(context.FunctionAppDirectory).AddJsonFile("local.settings.json", optional: true, reloadOnChange: false).AddEnvironmentVariables().Build();

            StreamReader reader = new StreamReader(myBlob);
            string fileContent = reader.ReadToEnd();
            if (!fileContent.Contains("#"))
            {
                log.LogInformation("Invalid File Content");
                return;
            }
            string[] split_FileContent = fileContent.Split(new[] { '#' }, 2);           

            log.LogInformation(fileContent);
            string customerType = split_FileContent[0];
            string message = split_FileContent[1];
            await SendMessageToBus(message, "text-file-topic", customerType);

            //log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
            //string a = "111111111111111";
            //return a;
        }

        private static async Task SendMessageToBus(string messageToSend, string topic, string customerType)
        {
            try
            {
                var message = new Message(Encoding.UTF8.GetBytes(messageToSend));
                message.CorrelationId = "ClientMessage";
                message.MessageId = Guid.NewGuid().ToString();
                message.UserProperties.Add("MessageFrom", customerType);
                // Create 2 Subscriptions and add the below Rule in each subscription
                // MessageFrom='Client1'
                // MessageFrom='Client2'
                var topicClient = new TopicClient(config["ServiceBusConnectionString"], topic);
                await topicClient.SendAsync(message);
                Console.WriteLine($"Message dropped to Service bus Topic {topic}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}
