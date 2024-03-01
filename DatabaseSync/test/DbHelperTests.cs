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

    // private async Task EmptyDatabaseAsync()
    // {
    //     await _dbHelper.EmptyDatabaseTableDboLogsAsync();
    // }

    // private async Task<string?> FindIdInsertedRowAsync()
    // {
    //     using (var connection = new SqlConnection(_testConnectionString))
    //     {
    //         await connection.OpenAsync();
    //         // there is only one row in the table, so no need to use a WHERE clause
    //         using (var command = new SqlCommand("SELECT * FROM dbo.Logs", connection))
    //         {
    //             using (var reader = await command.ExecuteReaderAsync())
    //             {
    //                 Console.WriteLine("reader.Read() " + reader.Read()); // Without this Console.WriteLine, the test will fail for some reason
    //                 var id = reader["Id"].ToString();
    //                 await connection.CloseAsync();
    //                 return id;
    //             }
    //         }
    //     }
    // }

    // [Fact]
    // public async Task TestInsertLogDataAsync()
    // {
    //     // Arrange
    //     await this.EmptyDatabaseAsync();
    //     string sampleMonth = "March";
    //     string sampleLogData = $"{{\"message\":\"Log entry\",\"severity\":\"info\"}}";

    //     // Act
    //     await _dbHelper.InsertLogDataAsync(sampleMonth, sampleLogData);

    //     // Assert
    //     using (var connection = new SqlConnection(_testConnectionString))
    //     {
    //         await connection.OpenAsync(); // TODO check if async will help, not needing Thread.Sleep anymore
    //         using (var command = new SqlCommand("SELECT * FROM dbo.Logs WHERE Month = @month AND LogData = @logData", connection))
    //         {
    //             command.Parameters.AddWithValue("@month", sampleMonth);
    //             command.Parameters.AddWithValue("@logData", sampleLogData);

    //             using (var reader = command.ExecuteReader())
    //             {
    //                 Assert.True(reader.Read(), "No data found with the provided month and log data");

    //                 var month = reader["Month"].ToString();
    //                 var logData = reader["LogData"].ToString();

    //                 Assert.Equal(sampleMonth, month);
    //                 Assert.Equal(sampleLogData, logData);
    //             }
    //         }
    //         connection.Close();
    //     }
    // }

    // [Fact]
    // public async Task TestUpdateLogDataAsync()
    // {
    //     // Arrange
    //     await this.EmptyDatabaseAsync();
    //     string sampleMonth = "December";
    //     string sampleLogData = $"{{\"message\":\"Log entry\",\"severity\":\"info\"}}";
    //     await _dbHelper.InsertLogDataAsync(sampleMonth, sampleLogData);
    //     string? id = await FindIdInsertedRowAsync() ?? string.Empty;

    //     // Act
    //     string updatedLogData = $"{{\"message\":\"Updated log entry\",\"severity\":\"info\"}}";
    //     await _dbHelper.UpdateLogDataAsync(sampleMonth, updatedLogData, int.Parse(id));

    //     // Assert
    //     using (var connection = new SqlConnection(_testConnectionString))
    //     {
    //         await connection.OpenAsync();
    //         using (var command = new SqlCommand("SELECT * FROM dbo.Logs WHERE Id = @Id", connection))
    //         {
    //             command.Parameters.AddWithValue("@Id", id);

    //             using (var reader = await command.ExecuteReaderAsync())
    //             {
    //                 Assert.True(reader.Read(), "No data found with the provided month and log data");

    //                 var month = reader["Month"].ToString();
    //                 var logData = reader["LogData"].ToString();

    //                 Assert.Equal(sampleMonth, month);
    //                 Assert.Equal(updatedLogData, logData);
    //                 Assert.Equal(id, reader["Id"].ToString());
    //             }
    //         }
    //         connection.Close();
    //     }
    // }

    // [Fact]
    // public async Task TestDeleteLogDataAsync()
    // {
    //     // Arrange
    //     await this.EmptyDatabaseAsync();
    //     string sampleMonth = "November";
    //     string sampleLogData = $"{{\"message\":\"Log entry\",\"severity\":\"info\"}}";
    //     await _dbHelper.InsertLogDataAsync(sampleMonth, sampleLogData);
    //     string? id = await FindIdInsertedRowAsync() ?? string.Empty;

    //     // Act
    //     await _dbHelper.DeleteLogDataAsync(int.Parse(id));

    //     // Assert
    //     using (var connection = new SqlConnection(_testConnectionString))
    //     {
    //         await connection.OpenAsync();
    //         using (var command = new SqlCommand("SELECT * FROM dbo.Logs WHERE Id = @Id", connection))
    //         {
    //             command.Parameters.AddWithValue("@Id", id);

    //             using (var reader = await command.ExecuteReaderAsync())
    //             {
    //                 Assert.False(reader.Read(), "Data found in the table dbo.Logs");
    //             }
    //         }
    //         connection.Close();
    //     }
    // }

    // [Fact]
    // public async Task TestEmptyDatabaseTableDboLogsAsync()
    // {
    //     // Act
    //     await _dbHelper.InsertLogDataAsync("March", "Log entry");
    //     await _dbHelper.EmptyDatabaseTableDboLogsAsync();

    //     // Assert
    //     using (var connection = new SqlConnection(_testConnectionString))
    //     {
    //         connection.Open();
    //         using (var command = new SqlCommand("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'cdc.dbo_Logs_CT'", connection))
    //         {
    //             var result = command.ExecuteScalar();
    //             Assert.Null(result);
    //         }
    //         connection.Close();
    //     }
    // }

    [Fact]
    public async Task TestGetOneDayOfDataFromAuditLogsTableAsync()
    {
        // Arrange
        await _dbHelper.EmptyDatabaseTableDboAuditLogsAsync();
        await _dbHelper.AddRowsToAuditLogTableWithCSVFileAsync("/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData.csv");
        string day = "2023-01-31";

        // Act
        var result = await _dbHelper.GetOneDayOfDataFromAuditLogsTableAsync(day);

        // Assert
        Assert.Equal(13705, result.Count);
    }

    [Fact]
    public async Task TestAddRowsToAuditLogTableWithCSVFileAsync() // 49 seconds with BulkInsert. 
    {
        // Arrange
        await _dbHelper.EmptyDatabaseTableDboAuditLogsAsync();
        // Get csv file from the assets folder in the project directory path
        string csvFilePath = "/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData.csv"; // TODO Change to Relative path eventually

        // Act
        await _dbHelper.AddRowsToAuditLogTableWithCSVFileAsync(csvFilePath);

        // Assert
        using (var connection = new SqlConnection(_testConnectionString))
        {
            await connection.OpenAsync();
            using (var command = new SqlCommand("SELECT COUNT(*) FROM AuditLog_20230101", connection))
            {
                var rowCount = (int)await command.ExecuteScalarAsync();
                Assert.Equal(1237548, rowCount);
            }
            connection.Close();
        }
    }
}