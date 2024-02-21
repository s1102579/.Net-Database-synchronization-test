

using Npgsql;

public class DbHelperPostgresql
{
    private readonly string connectionString;

    public DbHelperPostgresql(string connectionString)
    {
        this.connectionString = connectionString;
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
}