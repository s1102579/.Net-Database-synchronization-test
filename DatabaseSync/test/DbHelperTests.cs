using Xunit;
using System.Data;
using System.Linq;
using System.Data.SqlClient;
using Xunit.Abstractions;

public class DbHelperTests
{
    private readonly string _testConnectionString = "Server=localhost,1433;Database=MSSQL_LOG_TEST;User Id=sa;Password=Your_Strong_Password;";
    private readonly DbHelper _dbHelper;
    private readonly SqlServerDbContext _dbContext;
    private readonly ITestOutputHelper _output;

    public DbHelperTests(ITestOutputHelper output)
    {
        _output = output;
        _dbContext = new SqlServerDbContext();
        _dbHelper = new DbHelper(_dbContext, _testConnectionString);
    }

    private async Task EmptyDatabaseAsync()
    {
        await _dbHelper.EmptyDatabaseTableDboLogsAsync();
        Thread.Sleep(5000); // pollinginterval of the CDC is 5 seconds
        await _dbHelper.EmptyDatabaseCDCTableDboLogsAsync();
    }

    private async Task<string?> FindIdInsertedRowAsync()
    {

        using (var connection = new SqlConnection(_testConnectionString))
        {
            await connection.OpenAsync();
            // there is only one row in the table, so no need to use a WHERE clause
            using (var command = new SqlCommand("SELECT * FROM dbo.Logs", connection))
            {
                using (var reader = await command.ExecuteReaderAsync())
                {
                    Console.WriteLine("reader.Read() " + reader.Read()); // Without this Console.WriteLine, the test will fail for some reason
                    var id = reader["Id"].ToString();
                    await connection.CloseAsync();
                    return id;
                }
            }
        }

    }


    [Fact]
    public async Task TestInsertLogDataAsync()
    {
        // Arrange
        await this.EmptyDatabaseAsync();
        string sampleMonth = "March";
        string sampleLogData = $"{{\"message\":\"Log entry\",\"severity\":\"info\"}}";

        // Act
        await _dbHelper.InsertLogDataAsync(sampleMonth, sampleLogData);

        // Assert
        using (var connection = new SqlConnection(_testConnectionString))
        {
            await connection.OpenAsync(); // TODO check if async will help, not needing Thread.Sleep anymore
            using (var command = new SqlCommand("SELECT * FROM dbo.Logs WHERE Month = @month AND LogData = @logData", connection))
            {
                command.Parameters.AddWithValue("@month", sampleMonth);
                command.Parameters.AddWithValue("@logData", sampleLogData);

                using (var reader = command.ExecuteReader())
                {
                    Assert.True(reader.Read(), "No data found with the provided month and log data");

                    var month = reader["Month"].ToString();
                    var logData = reader["LogData"].ToString();

                    Assert.Equal(sampleMonth, month);
                    Assert.Equal(sampleLogData, logData);
                }
            }
            connection.Close();
        }
    }

    [Fact]
    public async Task TestUpdateLogDataAsync()
    {
        // Arrange
        await this.EmptyDatabaseAsync();
        string sampleMonth = "December";
        string sampleLogData = $"{{\"message\":\"Log entry\",\"severity\":\"info\"}}";
        await _dbHelper.InsertLogDataAsync(sampleMonth, sampleLogData);
        string? id = await FindIdInsertedRowAsync() ?? string.Empty;

        // Act
        string updatedLogData = $"{{\"message\":\"Updated log entry\",\"severity\":\"info\"}}";
        await _dbHelper.UpdateLogDataAsync(sampleMonth, updatedLogData, int.Parse(id));

        // Assert
        using (var connection = new SqlConnection(_testConnectionString))
        {
            await connection.OpenAsync();
            using (var command = new SqlCommand("SELECT * FROM dbo.Logs WHERE Id = @Id", connection))
            {
                command.Parameters.AddWithValue("@Id", id);

                using (var reader = await command.ExecuteReaderAsync())
                {
                    Assert.True(reader.Read(), "No data found with the provided month and log data");

                    var month = reader["Month"].ToString();
                    var logData = reader["LogData"].ToString();

                    Assert.Equal(sampleMonth, month);
                    Assert.Equal(updatedLogData, logData);
                    Assert.Equal(id, reader["Id"].ToString());
                }
            }
            connection.Close();
        }
    }

    [Fact]
    public async Task TestDeleteLogDataAsync()
    {
        // Arrange
        await this.EmptyDatabaseAsync();
        string sampleMonth = "November";
        string sampleLogData = $"{{\"message\":\"Log entry\",\"severity\":\"info\"}}";
        await _dbHelper.InsertLogDataAsync(sampleMonth, sampleLogData);
        string? id = await FindIdInsertedRowAsync() ?? string.Empty;

        // Act
        await _dbHelper.DeleteLogDataAsync(int.Parse(id));

        // Assert
        using (var connection = new SqlConnection(_testConnectionString))
        {
            await connection.OpenAsync();
            using (var command = new SqlCommand("SELECT * FROM dbo.Logs WHERE Id = @Id", connection))
            {
                command.Parameters.AddWithValue("@Id", id);

                using (var reader = await command.ExecuteReaderAsync())
                {
                    Assert.False(reader.Read(), "Data found in the table dbo.Logs");
                }
            }
            connection.Close();
        }
    }

    [Fact]
    public async Task TestEmptyDatabaseTableDboLogsAsync()
    {
        // Act
        await _dbHelper.InsertLogDataAsync("March", "Log entry");
        await _dbHelper.EmptyDatabaseTableDboLogsAsync();

        // Assert
        using (var connection = new SqlConnection(_testConnectionString))
        {
            connection.Open();
            using (var command = new SqlCommand("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'cdc.dbo_Logs_CT'", connection))
            {
                var result = command.ExecuteScalar();
                Assert.Null(result);
            }
            connection.Close();
        }
    }

    [Fact]
    public async Task TestEmptyDatabaseCDCTableDboLogsAsync()
    {
        // Act
        await _dbHelper.InsertLogDataAsync("March", "Log entry");
        Thread.Sleep(5000); // pollinginterval of the CDC is 5 seconds
        await _dbHelper.EmptyDatabaseCDCTableDboLogsAsync();

        // Assert
        using (var connection = new SqlConnection(_testConnectionString))
        {
            await connection.OpenAsync();
            using (var command = new SqlCommand("SELECT COUNT(*) FROM cdc.dbo_Logs_CT", connection))
            {
                var result = Convert.ToInt32(await command.ExecuteScalarAsync());
                Assert.Equal(0, result);
            }
            connection.Close();
        }
    }

    [Fact]
    public async Task TestQueryCDCTablesAsync()
    {
        // Arrange
        await this.EmptyDatabaseAsync();
        string sampleMonth = "March";
        string sampleLogData = $"{{\"message\":\"Log entry\",\"severity\":\"info\"}}";
        await _dbHelper.InsertLogDataAsync(sampleMonth, sampleLogData);
        Thread.Sleep(5000); // pollinginterval of the CDC is 5 seconds

        // Act
        var result = await DbHelper.QueryCDCTablesAsync(_testConnectionString);

        // Assert
        Assert.NotNull(result);
        Assert.Single(result.Tables);
        Assert.Equal("dbo.Logs", result.Tables[0].TableName); // already named the table for the postgreSQL database
        Assert.Equal(9, result.Tables[0].Columns.Count);
        Assert.Equal("__$operation", result.Tables[0].Columns[0].ColumnName);
        Assert.Equal("__$start_lsn", result.Tables[0].Columns[1].ColumnName);
        Assert.Equal("__$end_lsn", result.Tables[0].Columns[2].ColumnName);
        Assert.Equal("__$seqval", result.Tables[0].Columns[3].ColumnName);
        Assert.Equal("__$update_mask", result.Tables[0].Columns[4].ColumnName);
        Assert.Equal("Id", result.Tables[0].Columns[5].ColumnName);
        Assert.Equal("Month", result.Tables[0].Columns[6].ColumnName);
        Assert.Equal("LogData", result.Tables[0].Columns[7].ColumnName);
        Assert.Equal("__$command_id", result.Tables[0].Columns[8].ColumnName);

        Assert.Equal(1, result.Tables[0].Rows.Count); // assuming that only one row is added
        var row = result.Tables[0].Rows[0];
        Assert.Equal(2, row["__$operation"]); //check if the operation is an insert
        Assert.Equal(sampleMonth, row["Month"]);
        Assert.Equal(sampleLogData, row["LogData"]);
    }
}