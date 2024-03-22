using Xunit;
using System.Data;
using System.Linq;
using System.Data.SqlClient;
using Xunit.Abstractions;

public class DbHelperTests
{
    private readonly string _testConnectionString = "Server=localhost,1434;Database=MSSQL_LOG_TEST;User Id=sa;Password=Your_Strong_Password;";
    private readonly DbHelper _dbHelper;
    private readonly SqlServerDbContext _dbContext;
    private readonly ITestOutputHelper _output;

    public DbHelperTests(ITestOutputHelper output)
    {
        _output = output;
        _dbContext = SqlServerDbContext.Instance;
        _dbHelper = new DbHelper(_dbContext);
    }

    // [Fact]
    // public async Task TestGetOneDayOfDataFromAuditLogsTableAsync()
    // {
    //     // Arrange
    //     await _dbHelper.EmptyDatabaseTableDboAuditLogsAsync();
    //     await _dbHelper.AddRowsToAuditLogTableWithCSVFileAsync("/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData.csv");
    //     string day = "2023-01-31";

    //     // Act
    //     var result = await _dbHelper.GetOneDayOfDataFromAuditLogsTableAsync(day);

    //     // Assert
    //     Assert.Equal(13705, result.Count);
    // }

    [Fact]
    public async Task TestAddRowsToAuditLogTableWithCSVFileAsync()
    {
        // Create a new Stopwatch instance
        var stopwatch = new System.Diagnostics.Stopwatch();

        // Start the Stopwatch
        stopwatch.Start();

        // Arrange
        await _dbHelper.EmptyDatabaseTableDboAuditLogsAsync();
        // Get csv file from the assets folder in the project directory path
        string csvFilePath = "/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData.csv"; // TODO Change to Relative path eventually

        // Act
        await _dbHelper.AddRowsToAuditLogTableWithCSVFileAsync(csvFilePath);

        // Stop the Stopwatch
        stopwatch.Stop();

        // Write the elapsed time to the console
        Console.WriteLine($"Elapsed time: {stopwatch.Elapsed}");

        // Assert
        using (var connection = new SqlConnection(_testConnectionString))
        {
            await connection.OpenAsync();
            using (var command = new SqlCommand("SELECT COUNT(*) FROM AuditLog_20230101", connection))
            {
                object result = await command.ExecuteScalarAsync() ?? 0;
                long rowCount = result != null ? Convert.ToInt64(result) : 0;
                Assert.Equal(1237520, rowCount);
            }
            connection.Close();
        }
    }

    // [Fact]
    // public async Task TestAddRowsToAuditLogTableWithCSVFileAsync() // 45 seconds with BulkInsert. // 39 minutes and 32 seconds with SQLRawAsync // 27 seconds with SQLBulkCopy
    // {
    //     // Arrange
    //     await _dbHelper.EmptyDatabaseTableDboAuditLogsAsync();
    //     // Get csv file from the assets folder in the project directory path
    //     string csvFilePath = "/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData.csv"; // TODO Change to Relative path eventually

    //     // Act
    //     await _dbHelper.AddRowsToAuditLogTableWithCSVFileAsync(csvFilePath);

    //     // Assert
    //     using (var connection = new SqlConnection(_testConnectionString))
    //     {
    //         await connection.OpenAsync();
    //         using (var command = new SqlCommand("SELECT COUNT(*) FROM AuditLog_20230101", connection))
    //         {
    //             object result = await command.ExecuteScalarAsync() ?? 0;
    //             long rowCount = result != null ? Convert.ToInt64(result) : 0;
    //             Assert.Equal(1237520, rowCount);
    //         }
    //         connection.Close();
    //     }
    // }

    [Fact]
    public async Task TestGetAllDataFromAuditLogsTableAsync()
    {
        // Arrange
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        var result = await _dbHelper.GetDataFromAuditLogsTableAsync();

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