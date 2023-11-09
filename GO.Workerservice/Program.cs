using GO.Workerservice.Connection.Broker;
using GO.Workerservice.Connection.Client;

namespace GO.Workerservice
{
    public class Program
    {
        public static void Main(string[] args)
        {
            IHost host = Host.CreateDefaultBuilder(args)
                .ConfigureServices(services =>
                {
                    services.AddSingleton<MBroker>();
                    services.AddSingleton<MClient>();
                    services.AddHostedService<Worker>();
                })
                .Build();

            host.Run();
        }
    }
}