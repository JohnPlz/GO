namespace GO.Workerservice;

public sealed class DatabaseConfiguration
{
    public required string Host { get; set; } = null!;

    public int Port { get; set; } = 3306;

    public required string Username { get; set; } = null!;

    public string? Password { get; set; } = null;

    public required string Database { get; set; } = null!;

    internal string GenerateConnectionString()
    {
        // Server=myServerAddress;Port=1234;Database=myDataBase;Uid=myUsername;Pwd=myPassword;

        string connectionString = "";
        connectionString += $"Server={this.Host};";
        connectionString += $"Port={this.Port};";
        connectionString += $"Database={this.Database};";
        connectionString += $"Uid={this.Username};";
        if (this.Password is not null) {
            connectionString += $"Pwd={this.Password};";
        }
        return connectionString;  
    }
}