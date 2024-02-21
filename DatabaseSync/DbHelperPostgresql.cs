

using System.Data;
using Npgsql;
using Npgsql.Replication;

public class DbHelperPostgresql
{
    private readonly string connectionString;

    private static CDCTranslaterToPostgres cdcTranslaterToPostgres;

    public DbHelperPostgresql(string connectionString)
    {
        this.connectionString = connectionString;
        cdcTranslaterToPostgres = new CDCTranslaterToPostgres(); // later change to a instance of the actual class
    }

    public void checkIfDatabaseExists()
    {
        using (var connection = new NpgsqlConnection(connectionString))
        {
            connection.Open();

            Console.WriteLine("Checking if the database exists...");

            // Check if the database exists
            using (var checkDatabaseCommand = new NpgsqlCommand(
                "SELECT 1 FROM pg_database WHERE datname = 'postgres_sync_database'",
                connection))
            {
                var databaseExists = checkDatabaseCommand.ExecuteScalar();
                if (databaseExists == null)
                {
                    Console.WriteLine("Database does not exist. Creating...");
                    using (var createDatabaseCommand = new NpgsqlCommand(
                        "CREATE DATABASE postgres_sync_database",
                        connection))
                    {
                        createDatabaseCommand.ExecuteNonQuery();
                    }
                }
            }
        }
    }

    static void ExecuteQueryOnPostgreSQL(string query, string postgresqlConnectionString)
    {
        try
        {
            using (var connection = new NpgsqlConnection(postgresqlConnectionString))
            {
                connection.Open();

                using (var command = new NpgsqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }
            }

            Console.WriteLine($"Query executed on PostgreSQL: {query}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error executing query on PostgreSQL: {ex.Message}");
        }
    }



    public static void ApplyChangesToPostgreSQL(DataSet changes, string postgresqlConnectionString)
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
                if (postgresqlQuery != null) {
                    ExecuteQueryOnPostgreSQL(postgresqlQuery, postgresqlConnectionString);
                }
            }
        }
    }
}