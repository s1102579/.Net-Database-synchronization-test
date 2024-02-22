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

    private void emptyDatabase()
    {
        _dbHelper.emptyDatabaseTableDboLogs();
        _dbHelper.emptyDatabaseCDCTableDboLogs();
    }


    [Fact]
    public void TestInsertLogData()
    {
        // Arrange
        this.emptyDatabase();
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
        dbHelper.emptyDatabaseTableDboLogs();
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
        dbHelper.emptyDatabaseCDCTableDboLogs();
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
}