using Xunit;
using System.Data;
using System.Linq;
using System.Data.SqlClient;
using Xunit.Abstractions;

public class DbHelperTests
{
    private readonly string _testConnectionString = "Server=localhost,1433;Database=MSSQL_LOG_TEST;User Id=sa;Password=Your_Strong_Password;";
    private readonly DbHelper _dbHelper;
    private readonly ITestOutputHelper _output;

    public DbHelperTests(ITestOutputHelper output)
    {
        _output = output;
        _dbHelper = new DbHelper(_testConnectionString);
    }

    private void EmptyDatabase()
    {
        _dbHelper.EmptyDatabaseTableDboLogs();
        Thread.Sleep(4000);
        _dbHelper.EmptyDatabaseCDCTableDboLogs();
        Thread.Sleep(4000);
    }


    [Fact]
    public void TestInsertLogData()
    {
        // Arrange
        this.EmptyDatabase();
        string sampleMonth = "March";
        string sampleLogData = $"{{\"message\":\"Log entry\",\"severity\":\"info\"}}";

        // Act
        _dbHelper.InsertLogData(sampleMonth, sampleLogData);

        // Assert
        using (var connection = new SqlConnection(_testConnectionString))
        {
            connection.Open();
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
    public void TestEmptyDatabaseTableDboLogs()
    {
        // Arrange
        var dbHelper = new DbHelper(_testConnectionString);

        // Act
        dbHelper.EmptyDatabaseTableDboLogs();
        Thread.Sleep(4000);

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
    public void TestEmptyDatabaseCDCTableDboLogs()
    {
        // Arrange
        var dbHelper = new DbHelper(_testConnectionString);

        // Act
        dbHelper.EmptyDatabaseCDCTableDboLogs();
        Thread.Sleep(4000);

        // Assert
        using (var connection = new SqlConnection(_testConnectionString))
        {
            connection.Open();
            using (var command = new SqlCommand("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'dbo.Logs'", connection))
            {
                var result = command.ExecuteScalar();
                Assert.Null(result);
            }
            connection.Close();
        }
    }

    [Fact]
    public void TestQueryCDCTables()
    {
        // Arrange
        this.EmptyDatabase();
        string sampleMonth = "March";
        string sampleLogData = $"{{\"message\":\"Log entry\",\"severity\":\"info\"}}";
        _dbHelper.InsertLogData(sampleMonth, sampleLogData);
        Thread.Sleep(10000);

        // Act
        var result = DbHelper.QueryCDCTables(_testConnectionString);

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