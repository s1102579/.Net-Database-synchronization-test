

using System.Data;
using Npgsql;
using Xunit;

public class DbHelperPostgresqlTests
{
    private readonly string _testConnectionString = "Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;Database=postgres_sync_database;";
    private readonly DbHelperPostgresql _dbHelperPostgresql;

    public DbHelperPostgresqlTests()
    {
        _dbHelperPostgresql = new DbHelperPostgresql(_testConnectionString);
    }

    private void EmptyDatabase()
    {
        _dbHelperPostgresql.EmptyDatabaseTableDboLogs();
        Thread.Sleep(4000);
    }

    private void AddRowToDboLogs()
    {
        string tableName = "dbo.Logs";
        using (var connection = new NpgsqlConnection(_testConnectionString))
        {
            connection.Open();
            using var command = new NpgsqlCommand($"INSERT INTO \"{tableName}\" (\"Month\", \"LogData\") VALUES ('March', '{{\"message\":\"Log entry\",\"severity\":\"info\"}}')", connection);
            command.ExecuteNonQuery();
            connection.Close();
        }
    }

    [Fact]
    public void TestEmptyDatabaseTableDboLogs()
    {
        // Arrange
        this.AddRowToDboLogs();
        Thread.Sleep(4000);

        // Act
        _dbHelperPostgresql.EmptyDatabaseTableDboLogs();
        Thread.Sleep(4000);

        // Assert
        using (var connection = new NpgsqlConnection(_testConnectionString))
        {
            connection.Open();
            string tableName = "dbo.Logs";
            string commandText = $"SELECT * FROM \"{tableName}\";";
            using (var command = new NpgsqlCommand(commandText, connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    Assert.False(reader.Read(), "Data found in the table dbo.Logs");
                }
            }
            connection.Close();
        }
    }

    [Fact]
    public void TestApplyChangesToPostgreSQLInsert()
    {
        // Arrange
        this.EmptyDatabase();
        Thread.Sleep(4000);

        var dataSet = new DataSet();
        var dataTable = new DataTable("dbo.Logs");
        dataTable.Columns.Add("__$operation", typeof(int));
        dataTable.Columns.Add("__$start_lsn", typeof(string));
        dataTable.Columns.Add("__$seqval", typeof(string));
        dataTable.Columns.Add("__$update_mask", typeof(string));
        dataTable.Columns.Add("Id", typeof(int));
        dataTable.Columns.Add("Month", typeof(string));
        dataTable.Columns.Add("LogData", typeof(string));
        dataTable.Rows.Add(2, "0x000000330000000100010000", "0x000000330000000100010000", "0x000000330000000100010000", 1, "March", "{\"message\":\"Log entry\",\"severity\":\"info\"}");
        dataSet.Tables.Add(dataTable);

        var row = dataTable.NewRow();
        row["Id"] = 1;
        row["Month"] = "March";
        row["LogData"] = "{\"message\":\"Log entry\",\"severity\":\"info\"}";

        // Act
        DbHelperPostgresql.ApplyChangesToPostgreSQL(dataSet, _testConnectionString);
        Thread.Sleep(4000);

        // Assert
        using (var connection = new NpgsqlConnection(_testConnectionString))
        {
            connection.Open();
            string tableName = "dbo.Logs";
            string commandText = $"SELECT * FROM \"{tableName}\";";
            using (var command = new NpgsqlCommand(commandText, connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    Assert.True(reader.Read(), "Data is not found in the table dbo.Logs");
                    Assert.Equal(1, reader.GetInt32(0));
                    Assert.Equal("March", reader.GetString(1));
                    Assert.Equal("{\"message\": \"Log entry\", \"severity\": \"info\"}", reader.GetString(2));
                }
            }
            connection.Close();
        }
    }


}