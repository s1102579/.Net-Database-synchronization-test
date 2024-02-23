using System.Data;
using System.Data.SqlClient;
using Npgsql;
using Xunit;

public class DatabaseFixture
{
    public DataSet DataChanges { get; set; }
}

[CollectionDefinition("Database collection")]
public class DatabaseCollection : ICollectionFixture<DatabaseFixture>
{
    // This class has no code, and is never created. Its purpose is simply
    // to be the place to apply [CollectionDefinition] and all the
    // ICollectionFixture<> interfaces.
}

[TestCaseOrderer("test.PriorityOrderer", "DatabaseSync")]
[Collection("Database collection")]
public class IntegrationTests : IDisposable
{
    // Set up any resources needed for the tests
    private readonly string _connectionStringMSSQL = "Server=localhost,1433;Database=MSSQL_LOG_TEST;User Id=sa;Password=Your_Strong_Password;";
    private readonly string _connectionStringPostgres = "Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;Database=postgres_sync_database;";
    private readonly DbHelper _dbHelperMSSQL;
    private readonly DbHelperPostgresql _dbHelperPostgres;
    private DatabaseFixture fixture;

    public IntegrationTests(DatabaseFixture fixture)
    {
        this.fixture = fixture;
        _dbHelperMSSQL = new DbHelper(_connectionStringMSSQL);
        _dbHelperPostgres = new DbHelperPostgresql(_connectionStringPostgres);
    }

      // Clean up any resources used by the tests
    public void Dispose()
    {
        // TODO: Add your cleanup code here
    }

    private void EmptyDatabase()
    {
        _dbHelperMSSQL.EmptyDatabaseTableDboLogs();
        Thread.Sleep(4000);
        _dbHelperMSSQL.EmptyDatabaseCDCTableDboLogs();
        Thread.Sleep(4000);
        _dbHelperPostgres.EmptyDatabaseTableDboLogs();
    }

    [Fact, TestPriority(1)]
    public void TestaddingLogDataToMSSQL()
    {
        // Arrange
        string sampleMonth = "May";
        string sampleLogData = "{\"message\":\"Log entry 1\",\"severity\":\"info\"}";
        this.EmptyDatabase();
        Thread.Sleep(4000);

        // Act
        _dbHelperMSSQL.InsertLogData(sampleMonth, sampleLogData);

        // Assert
        using (var connection = new SqlConnection(_connectionStringMSSQL))
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

    [Fact, TestPriority(2)]
    public void TestCheckIfCDCTableIsUpdated()
    {
        // Act
        Thread.Sleep(10000);

        // Assert
        using (var connection = new SqlConnection(_connectionStringMSSQL))
        {
            connection.Open();
            using (var command = new SqlCommand("SELECT * FROM cdc.dbo_Logs_CT WHERE __$operation = 2", connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    Assert.True(reader.Read(), "No data found in the CDC table");

                    var month = reader["Month"].ToString();
                    var logData = reader["LogData"].ToString();

                    Assert.Equal("May", month); // take value from the previous test
                    Assert.Equal("{\"message\":\"Log entry 1\",\"severity\":\"info\"}", logData); // take value from the previous test
                }
            }
            connection.Close();
        }
    }

    [Fact, TestPriority(3)]
    public void TestQueryCDCTables()
    {
        // Act
        Thread.Sleep(1000);
        fixture.DataChanges = DbHelper.QueryCDCTables(_connectionStringMSSQL);
        Thread.Sleep(1000);

        // Assert
        Assert.NotNull(fixture.DataChanges);
        Assert.Single(fixture.DataChanges.Tables);
        Assert.Equal("dbo.Logs", fixture.DataChanges.Tables[0].TableName); // already named the table for the postgreSQL database
        Assert.Equal(9, fixture.DataChanges.Tables[0].Columns.Count);
        Assert.Equal("__$operation", fixture.DataChanges.Tables[0].Columns[0].ColumnName);
        Assert.Equal("__$start_lsn", fixture.DataChanges.Tables[0].Columns[1].ColumnName);
        Assert.Equal("__$end_lsn", fixture.DataChanges.Tables[0].Columns[2].ColumnName);
        Assert.Equal("__$seqval", fixture.DataChanges.Tables[0].Columns[3].ColumnName);
        Assert.Equal("__$update_mask", fixture.DataChanges.Tables[0].Columns[4].ColumnName);
        Assert.Equal("Id", fixture.DataChanges.Tables[0].Columns[5].ColumnName);
        Assert.Equal("Month", fixture.DataChanges.Tables[0].Columns[6].ColumnName);
        Assert.Equal("LogData", fixture.DataChanges.Tables[0].Columns[7].ColumnName);
        Assert.Equal("__$command_id", fixture.DataChanges.Tables[0].Columns[8].ColumnName);

        Assert.Equal(1, fixture.DataChanges.Tables[0].Rows.Count); // assuming that only one row is added
        var row = fixture.DataChanges.Tables[0].Rows[0];
        Assert.Equal(2, row["__$operation"]); //check if the operation is an insert
        Assert.Equal("May", row["Month"]);
        Assert.Equal("{\"message\":\"Log entry 1\",\"severity\":\"info\"}", row["LogData"]);
    }

    [Fact, TestPriority(4)]
    public void TestSyncDataToPostgres()
    {
        // Act
        Thread.Sleep(4000);
        DbHelperPostgresql.ApplyChangesToPostgreSQL(fixture.DataChanges, _connectionStringPostgres);
        Thread.Sleep(4000);

        // Assert
        using (var connection = new NpgsqlConnection(_connectionStringPostgres))
        {
            connection.Open();
            string tableName = "dbo.Logs";
            string commandText = $"SELECT * FROM \"{tableName}\";";
            using (var command = new NpgsqlCommand(commandText, connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    Assert.True(reader.Read(), "Data is not found in the table dbo.Logs");
                    Assert.Equal("May", reader.GetString(1));
                    Assert.Equal("{\"message\": \"Log entry 1\", \"severity\": \"info\"}", reader.GetString(2));
                }
            }
            connection.Close();
        }
    }
}
