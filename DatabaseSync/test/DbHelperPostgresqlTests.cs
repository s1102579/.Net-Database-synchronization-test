

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

    [Fact]
    public async Task TestAddRowsToAuditLogTableWithCSVFileAsync()
    {
        // Create a new Stopwatch instance
        var stopwatch = new System.Diagnostics.Stopwatch();

        // Start the Stopwatch
        stopwatch.Start();

        // Arrange
        await _dbHelperPostgresql.EmptyDatabaseTableAuditLogsAsync();
        // Get csv file from the assets folder in the project directory path
        string csvFilePath = "/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData.csv"; // TODO Change to Relative path eventually

        // Act
        await _dbHelperPostgresql.AddRowsToAuditLogTableWithCSVFileAsync(csvFilePath);

        // Stop the Stopwatch
        stopwatch.Stop();

        // Write the elapsed time to the console
        Console.WriteLine($"Elapsed time: {stopwatch.Elapsed}");

        // Assert
        using (var connection = new NpgsqlConnection(_testConnectionString))
        {
            await connection.OpenAsync();
            using (var command = new NpgsqlCommand("SELECT COUNT(*) FROM \"AuditLog_20230101\"", connection))
            {
                long rowCount = (long)(await command.ExecuteScalarAsync() ?? 0);
                Assert.Equal(1237520, rowCount);
            }
            connection.Close();
        }
    }

    [Fact]
    public async Task TestAddRowsToAuditLogTableWithCSVFileExceptForOneDayAsync() // bulkinsert runtime: 1m 2s  // runtime is: 20s with NPgsql binary import
    {
        // Arrange
        await _dbHelperPostgresql.EmptyDatabaseTableAuditLogsAsync();
        // Get csv file from the assets folder in the project directory path
        string csvFilePath = "/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData.csv"; // TODO Change to Relative path eventually

        // Act
        await _dbHelperPostgresql.AddRowsToAuditLogTableWithCSVFileExceptForOneDayAsync(csvFilePath);

        // Assert
        using (var connection = new NpgsqlConnection(_testConnectionString))
        {
            await connection.OpenAsync();
            using (var command = new NpgsqlCommand("SELECT COUNT(*) FROM \"AuditLog_20230101\"", connection))
            {
                long rowCount = (long)(await command.ExecuteScalarAsync() ?? 0);
                Assert.Equal(1237548 - 13705, rowCount); // 13705 is the amount of rows with januari 31st 2023
            }
            connection.Close();
        }
    }

    [Fact]
    public async Task TestEmptyDatabaseTableAuditLogPostgresAsync()
    {
        // Act
        await _dbHelperPostgresql.EmptyDatabaseTableAuditLogsAsync();

        // Assert
        using (var connection = new NpgsqlConnection(_testConnectionString))
        {
            await connection.OpenAsync();
            using (var command = new NpgsqlCommand("SELECT COUNT(*) FROM \"AuditLog_20230101\"", connection))
            {
                long rowCount = (long)(await command.ExecuteScalarAsync() ?? 0);
                Assert.Equal(0, rowCount);
            }
            connection.Close();
        }
    }

    [Fact]
    public async Task TestGetAllDataFromAuditLogsTableAsync()
    {
        // Arrange
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        var result = await _dbHelperPostgresql.GetDataFromAuditLogsTableAsync();

        // Stop timing after action
        stopwatch.Stop();

        // Assert
        Assert.Equal(1237520, result.Count);

        // Output the duration in milliseconds if the assertion is successful
        if (result.Count == 1237520)
        {
            Console.WriteLine($"Test duration: {stopwatch.Elapsed.TotalMilliseconds} ms");
        }
    }

}