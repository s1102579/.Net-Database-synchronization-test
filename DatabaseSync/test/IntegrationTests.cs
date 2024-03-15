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
    private readonly string _connectionStringMSSQL = "Server=localhost,1434;Database=MSSQL_LOG_TEST;User Id=sa;Password=Your_Strong_Password;TrustServerCertificate=True;";
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
        await _dbHelperPostgres.EmptyDatabaseTableAuditLogsAsync();
        await _dbHelperMSSQL.EmptyDatabaseTableDboAuditLogsAsync();
        // await _dbHelperPostgres.EmptyDatabaseTableDboLogsAsync();
    }

    [Fact, TestPriority(1)]
    public async Task TestAddingAuditLogDataToMSSQLAsync()
    {
        // Arrange
        await this.EmptyDatabaseAsync();
        // string csvFilePath = "/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData.csv"; // TODO Change to Relative path eventually
        string csvFilePath = "/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData202402.csv"; // TODO Change to Relative path eventually

        // Act
        await _dbHelperMSSQL.AddRowsToAuditLogTableWithCSVFileAsync(csvFilePath);

        // Assert
         using (var connection = new SqlConnection(_connectionStringMSSQL))
        {
            await connection.OpenAsync();
            using (var command = new SqlCommand("SELECT COUNT(*) FROM AuditLog_20230101", connection))
            {
                object result = await command.ExecuteScalarAsync() ?? 0;
                long rowCount = result != null ? Convert.ToInt64(result) : 0;
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
        Assert.Equal(1223818, fixture.DataChanges.Count);
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
                Assert.Equal(1223818, rowCount);
            }
            connection.Close();
        }
    }



    [Fact, TestPriority(5)]
    public async void TestEmptyDatabaseTableAuditLogPostgresAsync()
    {
        // Act
        await _dbHelperPostgres.EmptyDatabaseTableAuditLogsAsync();

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
        await _dbHelperPostgres.EmptyDatabaseTableAuditLogsAsync();
        // string csvFilePath = "/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData.csv"; // TODO Change to Relative path eventually
        string csvFilePath = "/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData202402.csv"; // TODO Change to Relative path eventually


        // Act
        await _dbHelperPostgres.AddRowsToAuditLogTableWithCSVFileExceptForOneDayAsync(csvFilePath);

        // Assert
         using (var connection = new NpgsqlConnection(_connectionStringPostgres))
        {
            await connection.OpenAsync();
            using (var command = new NpgsqlCommand("SELECT COUNT(*) FROM \"AuditLog_20230101\"", connection))
            {
                long rowCount = (long) (await command.ExecuteScalarAsync() ?? 0);
                Assert.Equal(1223818 - 13705, rowCount); // 13705 is the amount of rows with januari 31st 2023
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
                Assert.Equal(1223818, rowCount); // Assert that the postgres database has the same amount of rows as the source database
            }
            connection.Close();
        }
    }


    // [Fact, TestPriority(8)]
    // public async void TestEmptyDatabaseTableDboAuditLogsMSSQLAsync()
    // {
    //     // Act
    //     await _dbHelperMSSQL.EmptyDatabaseTableDboAuditLogsAsync();

    //     // Assert
    //     using (var connection = new SqlConnection(_connectionStringMSSQL))
    //     {
    //         await connection.OpenAsync();
    //         using (var command = new SqlCommand("SELECT * FROM AuditLog_20230101", connection))
    //         {
    //             using (var reader = await command.ExecuteReaderAsync())
    //             {
    //                 Assert.False(reader.Read(), "Data found in the table AuditLog_20230101");
    //             }
    //         }
    //         connection.Close();
    //     }
    // }
    
}
