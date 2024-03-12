using Xunit;
using System.Data;
using System.Linq;
using System.Data.SqlClient;
using Xunit.Abstractions;
using MySqlConnector;

public class DbHelperMySQLTests
{
    private readonly string _testConnectionString = "Server=localhost;Database=My_SQL_SPEED_TEST;User Id=root;Password=Your_Strong_Password;";
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
    public async Task TestAddRowsToAuditLogTableWithCSVFileAsync() // 45 seconds with BulkInsert. // 39 minutes and 32 seconds with SQLRawAsync // 27 seconds with SQLBulkCopy
    {
        // Arrange
        await _dbHelper.EmptyDatabaseTableAuditLogsAsync();
        // Get csv file from the assets folder in the project directory path
        string csvFilePath = "/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData.csv"; // TODO Change to Relative path eventually

        // Act
        await _dbHelper.AddRowsToAuditLogTableWithCSVFileAsync(csvFilePath);

        // Assert
        using (var connection = new MySqlConnection(_testConnectionString))
        {
            await connection.OpenAsync();
            using (var command = new MySqlCommand("SELECT COUNT(*) FROM `AuditLog_20230101`", connection))
            {
                object result = await command.ExecuteScalarAsync() ?? 0;
                long rowCount = result != null ? Convert.ToInt64(result) : 0;
                Assert.Equal(1237548, rowCount);
            }
            connection.Close();
        }
    }
}