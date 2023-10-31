namespace GO.Workerservice
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly DatabaseService? DatabaseService;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;


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
            //this.DatabaseService?.Connect();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                await Task.Delay(1000, stoppingToken);
            }
        }
    }
}