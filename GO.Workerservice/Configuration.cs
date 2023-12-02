namespace GO.Workerservice;

using Microsoft.Extensions.Logging;

public sealed class Configuration
{
    public required LogLevel LogLevel { get; set; }
    public required DatabaseConfiguration DatabaseConfiguration { get; set; } = null!;

    public required string ScanLocation { get; set; }

    public required string ExceptionFilePath { get; set; }

    public required float DefaultVolumeFactor { get; set; }

    public Dictionary<string, float>? ExceptionList { get; set; }
}