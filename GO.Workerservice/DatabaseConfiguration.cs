namespace GO.Workerservice;

public sealed class DatabaseConfiguration
{
    public required string Host { get; set; } = null!;

    public required string Username { get; set; } = null!;

    public string? Password { get; set; } = null;

    public required string Database { get; set; } = null!;
    public required string Driver { get; set; } = null!;

    public required string EngineName { get; set; } = null!;
    

    internal string GenerateConnectionString()
    {
        // Driver={SQL Anywhere 10};DatabaseName=godus;EngineName=test;uid=budde;pwd=PASSWORD;LINKs=tcpip(host=192.168.103.201)

        string connectionString = "";
        connectionString += "Driver={" + this.Driver + "};";
        connectionString += $"Database={this.Database};";
        connectionString += $"EngineName={this.EngineName};";
        connectionString += $"uid={this.Username};";
        if (this.Password is not null) {
            connectionString += $"pwd={this.Password};";
        }
        connectionString += $"LINKs=tcpip(host={this.Host});";

        return connectionString;  
    }
}