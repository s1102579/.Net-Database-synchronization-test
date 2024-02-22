

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


}