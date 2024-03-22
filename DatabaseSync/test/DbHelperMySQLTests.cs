using Xunit;
using System.Data;
using System.Linq;
using System.Data.SqlClient;
using Xunit.Abstractions;
using MySqlConnector;


public class DbHelperMySQLTests
{
    private readonly string _testConnectionString = "Server=localhost;Database=My_SQL_SPEED_TEST;User Id=root;Password=Your_Strong_Password;AllowLoadLocalInfile=true;AllowZeroDateTime=true;ConvertZeroDateTime=true;";
    private readonly DbHelperMySQL _dbHelper;
    private readonly MySQLDbContext _dbContext;
    private readonly ITestOutputHelper _output;

    public DbHelperMySQLTests(ITestOutputHelper output)
    {
        _output = output;
        _dbContext = MySQLDbContext.Instance;
        _dbHelper = new DbHelperMySQL(_dbContext);
    }

    [Fact]
    public async Task TestAddRowsToAuditLogTableWithCSVFileAsync()
    {
        // Create a new Stopwatch instance
        var stopwatch = new System.Diagnostics.Stopwatch();

        // Start the Stopwatch
        stopwatch.Start();

        // Arrange
        await _dbHelper.EmptyDatabaseTableAuditLogsAsync();
        // Get csv file from the assets folder in the project directory path
        string csvFilePath = "/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData.csv"; // TODO Change to Relative path eventually

        // Act
        await _dbHelper.AddRowsToAuditLogTableWithCSVFileAsync(csvFilePath);

        // Stop the Stopwatch
        stopwatch.Stop();

        // Write the elapsed time to the console
        Console.WriteLine($"Elapsed time: {stopwatch.Elapsed}");

        // Assert
        using (var connection = new MySqlConnection(_testConnectionString))
        {
            await connection.OpenAsync();
            using (var command = new MySqlCommand("SELECT COUNT(*) FROM `AuditLog_20230101`", connection))
            {
                object result = await command.ExecuteScalarAsync() ?? 0;
                long rowCount = result != null ? Convert.ToInt64(result) : 0;
                Assert.Equal(1237520, rowCount);
            }
            connection.Close();
        }
    }

    // [Fact]
    // public async Task TestAddRowsToAuditLogTableWithCSVFileAsync()
    // {
    //     // Arrange
    //     await _dbHelper.EmptyDatabaseTableAuditLogsAsync();
    //     // Get csv file from the assets folder in the project directory path
    //     string csvFilePath = "/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData.csv"; // TODO Change to Relative path eventually

    //     // Act
    //     await _dbHelper.AddRowsToAuditLogTableWithCSVFileAsync(csvFilePath);

    //     // Assert
    //     using (var connection = new MySqlConnection(_testConnectionString))
    //     {
    //         await connection.OpenAsync();
    //         using (var command = new MySqlCommand("SELECT COUNT(*) FROM `AuditLog_20230101`", connection))
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