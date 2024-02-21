using System.Data;

public class CDCTranslaterToPostgres
{
    static string TranslateToPostgreSQLQuery(DataRow change)
    {
        string tableName = change["Month"].ToString(); // Replace with the actual column name containing the table name

        switch (change.RowState)
        {
            case DataRowState.Added:
                return TranslateInsertToPostgreSQL(tableName, change);

            case DataRowState.Modified:
                return TranslateUpdateToPostgreSQL(tableName, change);

            case DataRowState.Deleted:
                return TranslateDeleteToPostgreSQL(tableName, change);

            default:
                throw new NotSupportedException($"Unsupported DataRowState: {change.RowState}");
        }
    }

    static string TranslateInsertToPostgreSQL(string tableName, DataRow change)
    {
        // Assuming change is a DataRow representing an insert operation
        // Create an INSERT query for PostgreSQL

        // Example:
        // INSERT INTO public."YourTable" (column1, column2, ...) VALUES (value1, value2, ...);

        var columns = string.Join(", ", change.Table.Columns.OfType<DataColumn>().Select(c => $"\"{c.ColumnName}\""));
        var values = string.Join(", ", change.ItemArray.Select(value => $"\'{value}\'"));

        return $"INSERT INTO public.\"{tableName}\" ({columns}) VALUES ({values});";
    }

    static string TranslateUpdateToPostgreSQL(string tableName, DataRow change)
    {
        // Assuming change is a DataRow representing an update operation
        // Create an UPDATE query for PostgreSQL

        // Example:
        // UPDATE public."YourTable" SET column1 = value1, column2 = value2, ... WHERE id = oldId;

        var setClause = string.Join(", ", change.Table.Columns.OfType<DataColumn>()
            .Select(c => $"\"{c.ColumnName}\" = \'{change[c, DataRowVersion.Current]}\'"));

        var whereClause = string.Join(" AND ", change.Table.PrimaryKey.Select(pk =>
            $"\"{pk.ColumnName}\" = \'{change[pk, DataRowVersion.Original]}\'"));

        return $"UPDATE public.\"{tableName}\" SET {setClause} WHERE {whereClause};";
    }

    static string TranslateDeleteToPostgreSQL(string tableName, DataRow change)
    {
        // Assuming change is a DataRow representing a delete operation
        // Create a DELETE query for PostgreSQL

        // Example:
        // DELETE FROM public."YourTable" WHERE id = oldId;

        var whereClause = string.Join(" AND ", change.Table.PrimaryKey.Select(pk =>
            $"\"{pk.ColumnName}\" = \'{change[pk, DataRowVersion.Original]}\'"));

        return $"DELETE FROM public.\"{tableName}\" WHERE {whereClause};";
    }

}

