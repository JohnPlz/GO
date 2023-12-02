using GO.Workerservice.Connection.Broker;
using GO.Workerservice.Connection.Client;

namespace GO.Workerservice
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly MBroker _broker;
        private readonly MClient _client;
        private readonly DatabaseService? DatabaseService;

        private readonly string ScanLocation;

        private readonly List<Dictionary<string, float>> ExceptionList;

        public Worker(ILogger<Worker> logger, MBroker broker, MClient client)
        {
            _logger = logger;
            _broker = broker;
            _client = client;

            ConfigurationReader reader = new();
            
            Configuration? configuration = ConfigurationReader.ReadConfiguration();

            if (configuration is null) {
                return;
            }
            
            this._logger.LogInformation("Configuration read.");

            string displayPassword =
                configuration!.DatabaseConfiguration.Password is null
                ? "-"
                : "***********";

            this._logger.LogDebug(
                "DatabaseConfiguration: \n\t" +
                $"Host: {configuration!.DatabaseConfiguration.Host} \n\t" +
                $"Port: {configuration!.DatabaseConfiguration.Port} \n\t" +
                $"Username: {configuration!.DatabaseConfiguration.Username} \n\t" +
                $"Password: {displayPassword} \n\t" +
                $"Database: {configuration!.DatabaseConfiguration.Database} \n"
            );

            this.DatabaseService = new(configuration!.DatabaseConfiguration, this._logger);
            this.DatabaseService?.Connect();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            StartBroker();
            await _client.Connect(stoppingToken);
            _client.OnReceivingMessage += NewMessage;

            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                await Task.Delay(TimeSpan.FromMinutes(30), stoppingToken);
            }
        }

        private void NewMessage(object? sender, string e) => Console.WriteLine(e ?? "no message"); //<= zum testen

        private async void StartBroker() => await _broker.RunBrokerAsync();
    }
}