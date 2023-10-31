using GO.Workerservice.Connection.Broker;

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
                    services.AddHostedService<Worker>();
                })
                .Build();

            host.Run();
        }
    }
}