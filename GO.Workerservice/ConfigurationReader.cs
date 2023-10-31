namespace GO.Workerservice;

using System;
using Microsoft.Extensions.Configuration;

public class ConfigurationReader
{
    public static Configuration? ReadConfiguration()
    {
        IConfigurationRoot builder = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .AddEnvironmentVariables()
            .Build();

        try
        {
            Configuration? configuration = builder.GetRequiredSection("Configuration").Get<Configuration>();

            return configuration;
        }
        catch (System.InvalidOperationException e)
        {
            Console.WriteLine("Invalid appsettings.json:");
            Console.WriteLine($"\t {e.Message}");
        }
        return null;
    }
}