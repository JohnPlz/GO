using GO.Workerservice.Connection.Broker;
using GO.Workerservice.Connection.Client;
using Microsoft.Extensions.Hosting;

namespace GO.Workerservice
{
    public class Program
    {
        public static void Main(string[] args)
        {
            if (Environment.OSVersion.Platform == PlatformID.Win32NT)
            {
                IHost host = Host.CreateDefaultBuilder(args)
                .ConfigureServices(services =>
                {
                    services.AddWindowsService(options => options.ServiceName = "GOService");
                    services.AddSingleton<MBroker>();
                    services.AddSingleton<MClient>();
                    services.AddHostedService<Worker>();
                })
                .Build();

                host.Run();
            }
            else
            {
                Environment.Exit(0); //logging?
            }
                
        }
    }
}