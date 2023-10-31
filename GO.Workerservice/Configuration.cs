namespace GO.Workerservice;

using Microsoft.Extensions.Logging;

public sealed class Configuration
{
    public required LogLevel LogLevel { get; set; }
    public required DatabaseConfiguration DatabaseConfiguration { get; set; } = null!;
}