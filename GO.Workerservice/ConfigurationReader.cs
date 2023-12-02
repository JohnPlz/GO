namespace GO.Workerservice;

using System;
using System.Globalization;
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

            configuration!.ExceptionList = ReadExceptionFile(configuration!.ExceptionFilePath);

            return configuration;
        }
        catch (System.InvalidOperationException e)
        {
            Console.WriteLine("Invalid appsettings.json:");
            Console.WriteLine($"\t {e.Message}");
        }
        return null;
    }

    private static Dictionary<string, float> ReadExceptionFile(string ExceptionFilePath) {
        String line;

        Dictionary<string, float> exceptionList = new();

        try
        {
            StreamReader sr = new(ExceptionFilePath);

            line = "";

            while (line != null)
            {
                line = sr.ReadLine();
                if (line == null) break;
                
                string[] parts = line.Split(';');
                string customerNr = parts[0];
                float volumeFactor = float.Parse(parts[1], CultureInfo.InvariantCulture.NumberFormat);
                exceptionList.Add(parts[0], volumeFactor);
            }
            sr.Close();
        }
        catch(Exception e)
        {
            Console.WriteLine("Exception: " + e.Message);
        }
        return exceptionList;
    }
}