

using System.Data;
using DatabaseSync.Entities;
using Npgsql;
using Xunit;

public class DbHelperPostgresqlTests
{
    private readonly string _testConnectionString = "Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;Database=postgres_sync_database;";
    private readonly DbHelperPostgresql _dbHelperPostgresql;
    private readonly PostgreSqlDbContext _dbContext;

    public DbHelperPostgresqlTests()
    {
        _dbContext = PostgreSqlDbContext.Instance;
        _dbHelperPostgresql = new DbHelperPostgresql(_testConnectionString, _dbContext);
    }

    private async Task EmptyDatabaseAsync()
    {
        await _dbHelperPostgresql.EmptyDatabaseTableDboLogsAsync();
    }

    private async Task AddRowToDboLogs()
    {
        string tableName = "dbo.Logs";
        using (var connection = new NpgsqlConnection(_testConnectionString))
        {
            await connection.OpenAsync();
            // using var command = new NpgsqlCommand($"INSERT INTO \"{tableName}\" (\"Month\", \"LogData\") VALUES ('March', '{{\"message\":\"Log entry\",\"severity\":\"info\"}}')", connection);
            using var command = new NpgsqlCommand($"INSERT INTO \"{tableName}\" (\"Id\", \"Month\", \"LogData\") VALUES (1, 'March', '{{\"message\":\"Log entry\",\"severity\":\"info\"}}')", connection);
            await command.ExecuteNonQueryAsync();
            connection.Close();
        }
    }

    [Fact]
    public async Task TestEmptyDatabaseTableDboLogsAsync()
    {
        // Arrange
        await this.EmptyDatabaseAsync();
        await this.AddRowToDboLogs();

        // Act
        await _dbHelperPostgresql.EmptyDatabaseTableDboLogsAsync();
        // Thread.Sleep(4000);

        // Assert
        using (var connection = new NpgsqlConnection(_testConnectionString))
        {
            await connection.OpenAsync();
            string tableName = "dbo.Logs";
            string commandText = $"SELECT * FROM \"{tableName}\";";
            using (var command = new NpgsqlCommand(commandText, connection))
            {
                using (var reader = await command.ExecuteReaderAsync())
                {
                    Assert.False(reader.Read(), "Data found in the table dbo.Logs");
                }
            }
            connection.Close();
        }
    }

    [Fact]
    public async void TestInsertListOfLogDataAsync()
    {
        // Arrange
        await this.EmptyDatabaseAsync();

        var logData = new List<Log>
        {
            new Log { Month = "March", LogData = "{\"message\": \"Log entry\", \"severity\": \"info\"}" },
            new Log { Month = "April", LogData = "{\"message\": \"Log entry 2\", \"severity\": \"info\"}"}
        };

        // Act
        await _dbHelperPostgresql.InsertListOfLogDataAsync(logData);

        // Assert
        using (var connection = new NpgsqlConnection(_testConnectionString))
        {
            await connection.OpenAsync();
            string tableName = "dbo.Logs";
            string commandText = $"SELECT * FROM \"{tableName}\";";
            using (var command = new NpgsqlCommand(commandText, connection))
            {
                using (var reader = await command.ExecuteReaderAsync())
                {
                    Assert.True(reader.Read(), "Data is not found in the table dbo.Logs");
                    Assert.Equal("March", reader.GetString(1));
                    Assert.Equal("{\"message\": \"Log entry\", \"severity\": \"info\"}", reader.GetString(2));
                    Assert.True(reader.Read(), "Data is not found in the table dbo.Logs");
                    Assert.Equal("April", reader.GetString(1));
                    Assert.Equal("{\"message\": \"Log entry 2\", \"severity\": \"info\"}", reader.GetString(2));
                }
            }
            connection.Close();
        }
    }
    


}