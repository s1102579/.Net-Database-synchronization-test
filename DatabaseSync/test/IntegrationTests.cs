using System.Data;
using System.Data.SqlClient;
using DatabaseSync.Entities;
using Npgsql;
using Xunit;

public class DatabaseFixture
{
    public List<AuditLog>? DataChanges { get; set; }
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
    private readonly string _connectionStringMSSQL = "Server=localhost,1434;Database=MSSQL_LOG_TEST;User Id=sa;Password=Your_Strong_Password;";
    private readonly string _connectionStringPostgres = "Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;Database=postgres_sync_database;";
    private readonly DbHelper _dbHelperMSSQL;
    private readonly DbHelperPostgresql _dbHelperPostgres;
    private readonly SqlServerDbContext _dbContext;
    private readonly PostgreSqlDbContext _dbContextPostgres;
    private DatabaseFixture fixture;

    public IntegrationTests(DatabaseFixture fixture)
    {
        this.fixture = fixture;
        _dbContext = SqlServerDbContext.Instance;
        _dbContextPostgres = PostgreSqlDbContext.Instance;
        _dbHelperMSSQL = new DbHelper(_dbContext);
        _dbHelperPostgres = new DbHelperPostgresql(_connectionStringPostgres, _dbContextPostgres);
    }

    // Clean up any resources used by the tests
    public void Dispose()
    {
        // TODO: Add your cleanup code here
    }

    private async Task EmptyDatabaseAsync()
    {
        // await _dbHelperMSSQL.EmptyDatabaseTableDboLogsAsync();
        await _dbHelperPostgres.EmptyDatabaseTableAudtLogsAsync();
        await _dbHelperMSSQL.EmptyDatabaseTableDboAuditLogsAsync();
        // await _dbHelperPostgres.EmptyDatabaseTableDboLogsAsync();
    }

    [Fact, TestPriority(1)]
    public async Task TestAddingAuditLogDataToMSSQLAsync()
    {
        // Arrange
        await this.EmptyDatabaseAsync();
        string csvFilePath = "/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData.csv"; // TODO Change to Relative path eventually

        // Act
        await _dbHelperMSSQL.AddRowsToAuditLogTableWithCSVFileAsync(csvFilePath);

        // Assert
         using (var connection = new SqlConnection(_connectionStringMSSQL))
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

    [Fact, TestPriority(2)]
    public async Task TestGetAllDataFromAuditLogTableMSSQLAsync()
    {
        // Act
        fixture.DataChanges = await _dbHelperMSSQL.GetDataFromAuditLogsTableAsync();

        // Assert
        Assert.NotNull(fixture.DataChanges);
        Assert.Equal(1237548, fixture.DataChanges.Count);
    }

    [Fact, TestPriority(3)]
    public async void TestSyncDataToPostgresInsertAuditLog()
    {
        // Act
        if (fixture.DataChanges != null)
        {
            await _dbHelperPostgres.InsertListOfAuditLogDataAsync(fixture.DataChanges);
        }

        // Assert
        using (var connection = new NpgsqlConnection(_connectionStringPostgres))
        {
            await connection.OpenAsync();
            string tableName = "AuditLog_20230101";
            string commandText = $"SELECT COUNT(*) FROM \"{tableName}\";";
            using (var command = new NpgsqlCommand(commandText, connection))
            {
                long rowCount = (long)(await command.ExecuteScalarAsync() ?? 0);
                Assert.Equal(1237548, rowCount);
            }
            connection.Close();
        }
    }



    [Fact, TestPriority(5)]
    public async void TestEmptyDatabaseTableAuditLogPostgresAsync()
    {
        // Act
        await _dbHelperPostgres.EmptyDatabaseTableAudtLogsAsync();

        // Assert
        using (var connection = new NpgsqlConnection(_connectionStringPostgres))
        {
            await connection.OpenAsync();
            string tableName = "AuditLog_20230101";
            string commandText = $"SELECT COUNT(*) FROM \"{tableName}\";";
            using (var command = new NpgsqlCommand(commandText, connection))
            {
                long rowCount = (long)(await command.ExecuteScalarAsync() ?? 0);
                Assert.Equal(0, rowCount);
            }
            connection.Close();
        }
    }

    [Fact, TestPriority(6)]
    public async void TestAddRowsToAuditLogTableWithCSVFileExceptForOneDayAsync()
    {
        // Arrange
        await _dbHelperPostgres.EmptyDatabaseTableAudtLogsAsync();
        string csvFilePath = "/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData.csv"; // TODO Change to Relative path eventually

        // Act
        await _dbHelperPostgres.AddRowsToAuditLogTableWithCSVFileExceptForOneDayAsync(csvFilePath);

        // Assert
         using (var connection = new NpgsqlConnection(_connectionStringPostgres))
        {
            await connection.OpenAsync();
            using (var command = new NpgsqlCommand("SELECT COUNT(*) FROM \"AuditLog_20230101\"", connection))
            {
                long rowCount = (long) (await command.ExecuteScalarAsync() ?? 0);
                Assert.Equal(1237548 - 13705, rowCount); // 13705 is the amount of rows with januari 31st 2023
            }
            connection.Close();
        }
    }

    [Fact, TestPriority(7)]
    public async void TestSyncOneDayOfDataWithPostgresAsync() // The postgresql database is one day behind the source database and needs to be synced with the previous day's data
    {
        // Arrange
        string day = "2023-01-31";

        // Act
        List<AuditLog> auditLogs = await _dbHelperMSSQL.GetOneDayOfDataFromAuditLogsTableAsync(day);
        await _dbHelperPostgres.InsertListOfAuditLogDataAsync(auditLogs);

        // Assert
        using (var connection = new NpgsqlConnection(_connectionStringPostgres))
        {
            await connection.OpenAsync();
            using (var command = new NpgsqlCommand("SELECT COUNT(*) FROM \"AuditLog_20230101\"", connection))
            {
                long rowCount = (long) (await command.ExecuteScalarAsync() ?? 0);
                Assert.Equal(1237548, rowCount); // Assert that 
            }
            connection.Close();
        }
    }




    [Fact, TestPriority(8)]
    public async void TestEmptyDatabaseTableDboAuditLogsMSSQLAsync()
    {
        // Act
        await _dbHelperMSSQL.EmptyDatabaseTableDboAuditLogsAsync();

        // Assert
        using (var connection = new SqlConnection(_connectionStringMSSQL))
        {
            await connection.OpenAsync();
            using (var command = new SqlCommand("SELECT * FROM AuditLog_20230101", connection))
            {
                using (var reader = await command.ExecuteReaderAsync())
                {
                    Assert.False(reader.Read(), "Data found in the table AuditLog_20230101");
                }
            }
            connection.Close();
        }
    }
    

    // [Fact, TestPriority(1)]
    // public async Task TestaddingLogDataToMSSQLAsync()
    // {
    //     // Arrange
    //     string sampleMonth = "May";
    //     string sampleLogData = "{\"message\":\"Log entry 1\",\"severity\":\"info\"}";
    //     await this.EmptyDatabaseAsync();

    //     // Act
    //     await _dbHelperMSSQL.InsertLogDataAsync(sampleMonth, sampleLogData);

    //     // Assert
    //     using (var connection = new SqlConnection(_connectionStringMSSQL))
    //     {
    //         await connection.OpenAsync();
    //         using (var command = new SqlCommand("SELECT * FROM dbo.Logs WHERE Month = @month AND LogData = @logData", connection))
    //         {
    //             command.Parameters.AddWithValue("@month", sampleMonth);
    //             command.Parameters.AddWithValue("@logData", sampleLogData);

    //             using (var reader = await command.ExecuteReaderAsync())
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

    // [Fact, TestPriority(2)]
    // public async void TestSyncDataToPostgresInsert()
    // {
    //     // Arrange
    //     // Get data from mssql database in the logs table not using the cdc table
    //     List<Log> logs = await _dbHelperMSSQL.GetDataFromLogsTableAsync();

    //     // Act
    //     if (logs != null)
    //     {
    //         await _dbHelperPostgres.InsertListOfLogDataAsync(logs);
    //     }

    //     // Assert
    //     using (var connection = new NpgsqlConnection(_connectionStringPostgres))
    //     {
    //         await connection.OpenAsync();
    //         string tableName = "dbo.Logs";
    //         string commandText = $"SELECT * FROM \"{tableName}\";";
    //         using (var command = new NpgsqlCommand(commandText, connection))
    //         {
    //             using (var reader = await command.ExecuteReaderAsync())
    //             {
    //                 Assert.True(reader.Read(), "Data is not found in the table dbo.Logs");
    //                 Assert.Equal(logs[0]?.Id, reader.GetInt32(0));
    //                 Assert.Equal("May", reader.GetString(1));
    //                 Assert.Equal("{\"message\":\"Log entry 1\",\"severity\":\"info\"}", reader.GetString(2));
    //             }
    //         }
    //         connection.Close();
    //     }
    // }

    // [Fact, TestPriority(3)]
    // public async void TestEmptyDatabaseTableDboLogsMSSQLAsync()
    // {
    //     // Act
    //     // await _dbHelperMSSQL.EmptyDatabaseTableDboLogsAsync();

    //     // Assert
    //     using (var connection = new SqlConnection(_connectionStringMSSQL))
    //     {
    //         await connection.OpenAsync();
    //         using (var command = new SqlCommand("SELECT * FROM dbo.Logs", connection))
    //         {
    //             using (var reader = await command.ExecuteReaderAsync())
    //             {
    //                 Assert.False(reader.Read(), "Data found in the table dbo.Logs");
    //             }
    //         }
    //         connection.Close();
    //     }
    // }

    // [Fact, TestPriority(4)]
    // public async void TestEmptyDatabaseTableDboLogsPostgresAsync()
    // {
    //     // Act
    //     await _dbHelperPostgres.EmptyDatabaseTableDboLogsAsync();

    //     // Assert
    //     using (var connection = new NpgsqlConnection(_connectionStringPostgres))
    //     {
    //         await connection.OpenAsync();
    //         string tableName = "dbo.Logs";
    //         string commandText = $"SELECT * FROM \"{tableName}\";";
    //         using (var command = new NpgsqlCommand(commandText, connection))
    //         {
    //             using (var reader = await command.ExecuteReaderAsync())
    //             {
    //                 Assert.False(reader.Read(), "Data found in the table dbo.Logs");
    //             }
    //         }
    //         connection.Close();
    //     }
    // }

    // [Fact, TestPriority(5)]
    // public async void TestInsertLargeListOfLogDataMSSQLAsync()
    // {
    //     // Arrange
    //     await this.EmptyDatabaseAsync();

    //     // make a list of 10000 entries of logData
    //     List<Log> logData = new List<Log>();
    //     for (int i = 0; i < 10000; i++)
    //     {
    //         logData.Add(new Log { Month = "May", LogData = $"{{\"message\":\"Log entry {i}\",\"severity\":\"info\"}}" });
    //     }

    //     // Act
    //     await _dbHelperMSSQL.InsertListOfLogDataAsync(logData);

    //     // Assert
    //     using (var connection = new SqlConnection(_connectionStringMSSQL))
    //     {
    //         await connection.OpenAsync();
    //         using (var command = new SqlCommand("SELECT COUNT(*) FROM dbo.Logs", connection))
    //         {
    //             int rowCount = (int)(await command.ExecuteScalarAsync() ?? 0);
    //             Assert.Equal(10000, rowCount);
    //         }
    //         connection.Close();
    //     }
    // }

    // [Fact, TestPriority(6)]
    // public async void TestSyncDataToPostgresInsertLarge()
    // {
    //     // Arrange
    //     // Get data from mssql database in the logs table not using the cdc table
    //     List<Log> logs = await _dbHelperMSSQL.GetDataFromLogsTableAsync();

    //     // Act
    //     if (logs != null)
    //     {
    //         await _dbHelperPostgres.InsertListOfLogDataAsync(logs);
    //     }

    //     // Assert
    //     using (var connection = new NpgsqlConnection(_connectionStringPostgres))
    //     {
    //         await connection.OpenAsync();
    //         string tableName = "dbo.Logs";
    //         string commandText = $"SELECT COUNT(*) FROM \"{tableName}\";";
    //         using (var command = new NpgsqlCommand(commandText, connection))
    //         {
    //             long rowCount = (long)(await command.ExecuteScalarAsync() ?? 0);
    //             Assert.Equal(10000, rowCount);
    //         }
    //         connection.Close();
    //     }
    // }
}
