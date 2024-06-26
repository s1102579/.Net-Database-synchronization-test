

using System.Data;
using Npgsql;
using Npgsql.Replication;

public class DbHelperPostgresql
{
    private readonly string connectionString;

    // private static CDCTranslaterToPostgres cdcTranslaterToPostgres;

    public DbHelperPostgresql(string connectionString)
    {
        this.connectionString = connectionString;
        // cdcTranslaterToPostgres = new CDCTranslaterToPostgres(); // later change to a instance of the actual class
    }

    public async void CheckIfDatabaseExists()
    {
        using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();

            Console.WriteLine("Checking if the database exists...");

            // Check if the database exists
            using (var checkDatabaseCommand = new NpgsqlCommand(
                "SELECT 1 FROM pg_database WHERE datname = 'postgres_sync_database'",
                connection))
            {
                var databaseExists = await checkDatabaseCommand.ExecuteScalarAsync();
                if (databaseExists == null)
                {
                    Console.WriteLine("Database does not exist. Creating...");
                    using (var createDatabaseCommand = new NpgsqlCommand(
                        "CREATE DATABASE postgres_sync_database",
                        connection))
                    {
                        await createDatabaseCommand.ExecuteNonQueryAsync();
                    }
                }
            }
            connection.Close();
        }
    }

    public async Task EmptyDatabaseTableDboLogsAsync()
    {
        string tableName = "dbo.Logs";
        string commandText = $"DELETE FROM \"{tableName}\";";
        using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();

            using (var command = new NpgsqlCommand(commandText, connection))
            {
                await command.ExecuteNonQueryAsync();
            }

            connection.Close();
        }
    }


    public async Task CreateLogsTableAsync()
    {
        using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();

            // Check if the Logs table already exists
            using (var checkTableCommand = new NpgsqlCommand(
                "SELECT 1 FROM pg_tables WHERE tablename = 'Logs'",
                connection))
            {
                var tableExists = await checkTableCommand.ExecuteScalarAsync();
                if (tableExists == null)
                {
                    Console.WriteLine("Table does not exist. Creating...");
                    using (var createTableCommand = new NpgsqlCommand(
                        "CREATE TABLE Logs (Id SERIAL PRIMARY KEY, Month VARCHAR(50), LogData JSONB)",
                        connection))
                    {
                        await createTableCommand.ExecuteNonQueryAsync();
                    }
                }
            }
            connection.Close();
        }
    }

    static async Task ExecuteQueryOnPostgreSQL(string query, string postgresqlConnectionString)
    {
        try
        {
            using (var connection = new NpgsqlConnection(postgresqlConnectionString))
            {
                await connection.OpenAsync();

                using (var command = new NpgsqlCommand(query, connection))
                {
                    await command.ExecuteNonQueryAsync();
                }
                connection.Close();
            }

            Console.WriteLine($"Query executed on PostgreSQL: {query}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error executing query on PostgreSQL: {ex.Message}");
        }
    }



    public static async Task ApplyChangesToPostgreSQLAsync(DataSet changes, string postgresqlConnectionString)
    {
        // Iterate through the DataTables inside the DataSet
        foreach (DataTable table in changes.Tables)
        {
            // Iterate through the rows of each DataTable
            foreach (DataRow row in table.Rows)
            {
                // Translate MSSQL-specific syntax to PostgreSQL syntax
                string postgresqlQuery = CDCTranslaterToPostgres.TranslateToPostgreSQLQuery(row, table.TableName);

                Console.WriteLine($"PostgreSQL query: {postgresqlQuery}");

                // Execute the query on the PostgreSQL database
                if (postgresqlQuery != null)
                {
                    await ExecuteQueryOnPostgreSQL(postgresqlQuery, postgresqlConnectionString);
                }
            }
        }
    }
}