using System.Data;
using System.Data.SqlClient;
using Npgsql;
using Xunit;

public class DatabaseFixture
{
    public DataSet? DataChanges { get; set; }
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

    private async Task EmptyDatabaseAsync()
    {
        await _dbHelperMSSQL.EmptyDatabaseTableDboLogsAsync();
        Thread.Sleep(5000); // pollinginterval of the CDC is 5 seconds
        await _dbHelperMSSQL.EmptyDatabaseCDCTableDboLogsAsync();
        await _dbHelperPostgres.EmptyDatabaseTableDboLogsAsync();
    }

    [Fact, TestPriority(1)]
    public async Task TestaddingLogDataToMSSQLAsync()
    {
        // Arrange
        string sampleMonth = "May";
        string sampleLogData = "{\"message\":\"Log entry 1\",\"severity\":\"info\"}";
        await this.EmptyDatabaseAsync();

        // Act
        await _dbHelperMSSQL.InsertLogDataAsync(sampleMonth, sampleLogData);

        // Assert
        using (var connection = new SqlConnection(_connectionStringMSSQL))
        {
            await connection.OpenAsync();
            using (var command = new SqlCommand("SELECT * FROM dbo.Logs WHERE Month = @month AND LogData = @logData", connection))
            {
                command.Parameters.AddWithValue("@month", sampleMonth);
                command.Parameters.AddWithValue("@logData", sampleLogData);

                using (var reader = await command.ExecuteReaderAsync())
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
    public async Task TestCheckIfCDCTableIsUpdatedAsync()
    {
        // Act
        Thread.Sleep(5000); // pollinginterval of the CDC is 5 seconds

        // Assert
        using (var connection = new SqlConnection(_connectionStringMSSQL))
        {
            await connection.OpenAsync();
            using (var command = new SqlCommand("SELECT * FROM cdc.dbo_Logs_CT WHERE __$operation = 2", connection))
            {
                using (var reader = await command.ExecuteReaderAsync())
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
    public async void TestQueryCDCTables()
    {
        // Act
        fixture.DataChanges = await DbHelper.QueryCDCTablesAsync(_connectionStringMSSQL);

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
    public async void TestSyncDataToPostgresInsert()
    {
        // Act
        if (fixture.DataChanges != null)
        {
            await DbHelperPostgresql.ApplyChangesToPostgreSQLAsync(fixture.DataChanges, _connectionStringPostgres);
        }

        // Assert
        using (var connection = new NpgsqlConnection(_connectionStringPostgres))
        {
            await connection.OpenAsync();
            string tableName = "dbo.Logs";
            string commandText = $"SELECT * FROM \"{tableName}\";";
            using (var command = new NpgsqlCommand(commandText, connection))
            {
                using (var reader = await command.ExecuteReaderAsync())
                {
                    Assert.True(reader.Read(), "Data is not found in the table dbo.Logs");
                    Assert.Equal("May", reader.GetString(1));
                    Assert.Equal("{\"message\": \"Log entry 1\", \"severity\": \"info\"}", reader.GetString(2));
                }
            }
            connection.Close();
        }
    }

    [Fact, TestPriority(5)]
    public async Task TestUpdateLogDataInMSSQLAsync() // will fail for now
    {
        // Arrange
        string sampleMonth = "June";
        string sampleLogData = "{\"message\":\"Log entry 2\",\"severity\":\"info\"}";

        // Act
        if (fixture.DataChanges != null && fixture.DataChanges.Tables[0].Rows.Count > 0)
        {
            var id = fixture.DataChanges.Tables[0].Rows[0]["Id"]?.ToString();
            if (id != null)
            {
                await _dbHelperMSSQL.UpdateLogDataAsync(sampleMonth, sampleLogData, id);
            }
        }

        // Assert
        using (var connection = new SqlConnection(_connectionStringMSSQL))
        {
            await connection.OpenAsync();
            using (var command = new SqlCommand("SELECT * FROM dbo.Logs WHERE Id = @Id", connection))
            {
                if (fixture.DataChanges != null && fixture.DataChanges.Tables[0].Rows.Count > 0)
                {
                    command.Parameters.AddWithValue("@Id", fixture.DataChanges.Tables[0].Rows[0]["Id"]);
                }

                using (var reader = await command.ExecuteReaderAsync())
                {
                    Assert.True(reader.Read(), "No data found with the provided month and log data");

                    var id = reader["Id"]?.ToString();
                    var month = reader["Month"]?.ToString();
                    var logData = reader["LogData"]?.ToString();

                    Assert.Equal(fixture.DataChanges?.Tables[0].Rows[0]["Id"]?.ToString(), id);
                    Assert.Equal(sampleMonth, month);
                    Assert.Equal(sampleLogData, logData);
                }
            }
            connection.Close();
        }
    }

    [Fact, TestPriority(6)]
    public async void TestQueryCDCTablesAfterUpdate()
    {
        // Arrange
        Thread.Sleep(5000); // wait for the CDC table to be updated

        // Act
        fixture.DataChanges = await DbHelper.QueryCDCTablesAsync(_connectionStringMSSQL);

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

        Assert.Equal(3, fixture.DataChanges.Tables[0].Rows.Count); // assuming that only 2 rows are added and 1 was already there
        var oldUpdaterow = fixture.DataChanges.Tables[0].Rows[1];
        var newUpdaterow = fixture.DataChanges.Tables[0].Rows[2];
        Assert.Equal(3, oldUpdaterow["__$operation"]); //check if the operation is an update (old data)
        Assert.Equal("May", oldUpdaterow["Month"]);
        Assert.Equal("{\"message\":\"Log entry 1\",\"severity\":\"info\"}", oldUpdaterow["LogData"]);

        Assert.Equal(4, newUpdaterow["__$operation"]); //check if the operation is an update (new data)
        Assert.Equal("June", newUpdaterow["Month"]);
        Assert.Equal("{\"message\":\"Log entry 2\",\"severity\":\"info\"}", newUpdaterow["LogData"]);
    }

    [Fact, TestPriority(7)]
    public async void TestSyncDataToPostgresUpdate()
    {
        // Act
        if (fixture.DataChanges != null)
        {
            await DbHelperPostgresql.ApplyChangesToPostgreSQLAsync(fixture.DataChanges, _connectionStringPostgres);
        }

        // Assert
        using (var connection = new NpgsqlConnection(_connectionStringPostgres))
        {
            await connection.OpenAsync();
            string tableName = "dbo.Logs";
            string commandText = $"SELECT * FROM \"{tableName}\";";
            using (var command = new NpgsqlCommand(commandText, connection))
            {
                using (var reader = await command.ExecuteReaderAsync())
                {
                    Assert.True(reader.Read(), "Data is not found in the table dbo.Logs");
                    Assert.Equal("June", reader.GetString(1));
                    Assert.Equal("{\"message\": \"Log entry 2\", \"severity\": \"info\"}", reader.GetString(2));
                }
            }
            connection.Close();
        }
    }

    [Fact, TestPriority(8)]
    public async Task TestDeleteLogDataInMSSQLAsync()
    {
        // Act
        if (fixture.DataChanges != null && fixture.DataChanges.Tables[0].Rows.Count > 2)
        {
            var id = fixture.DataChanges.Tables[0].Rows[2]["Id"]?.ToString();
            if (id != null)
            {
                await _dbHelperMSSQL.DeleteLogDataAsync(id);
            }
        }

        // Assert
        using (var connection = new SqlConnection(_connectionStringMSSQL))
        {
            await connection.OpenAsync();
            using (var command = new SqlCommand("SELECT * FROM dbo.Logs WHERE Id = @Id", connection))
            {
                var idValue = fixture.DataChanges?.Tables[0].Rows[2]["Id"];
                if (idValue != null)
                {
                    command.Parameters.AddWithValue("@Id", idValue);
                }
       
                using (var reader = await command.ExecuteReaderAsync())
                {
                    Assert.False(reader.Read(), "Data found with the provided Id");
                }
            }
            connection.Close();
        }
    }

    [Fact, TestPriority(9)]
    public async void TestQueryCDCTablesAfterDelete()
    {
        // Arrange
        Thread.Sleep(5000); // wait for the CDC table to be updated

        // Act
        fixture.DataChanges = await DbHelper.QueryCDCTablesAsync(_connectionStringMSSQL);

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

        Assert.Equal(4, fixture.DataChanges.Tables[0].Rows.Count); // assuming that only 1 rows are added and 3 was already there
        var deleterow = fixture.DataChanges.Tables[0].Rows[3];
        Assert.Equal(1, deleterow["__$operation"]); //check if the operation is an delete operation
        Assert.Equal("June", deleterow["Month"]);
        Assert.Equal("{\"message\":\"Log entry 2\",\"severity\":\"info\"}", deleterow["LogData"]);
    }

    [Fact, TestPriority(10)]
    public async void TestSyncDataToPostgresDelete()
    {
        // Act
        if (fixture.DataChanges != null)
        {
            await DbHelperPostgresql.ApplyChangesToPostgreSQLAsync(fixture.DataChanges, _connectionStringPostgres);
        }

        // Assert
        using (var connection = new NpgsqlConnection(_connectionStringPostgres))
        {
            await connection.OpenAsync();
            string tableName = "dbo.Logs";
            string commandText = $"SELECT * FROM \"{tableName}\";";
            using (var command = new NpgsqlCommand(commandText, connection))
            {
                using (var reader = await command.ExecuteReaderAsync())
                {
                    Assert.False(reader.Read(), "Data found in the table dbo.Logs");
                }
            }
            connection.Close();
        }
    }
}
