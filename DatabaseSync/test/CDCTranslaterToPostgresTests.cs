using System.Data;
using Xunit;
using Moq;

public class CDCTranslaterToPostgresTests
{
    [Fact]
    public void TestTranslateInsertToPostgreSQL()
    {
        // Arrange
        var mockTable = new DataTable();
        mockTable.Columns.Add("Id", typeof(int));
        mockTable.Columns.Add("Name", typeof(string));
        mockTable.Columns.Add("__$operation", typeof(int));

        var row = mockTable.NewRow();
        row["Id"] = 1;
        row["Name"] = "Test";
        row["__$operation"] = 2;

        // Act
        var result = CDCTranslaterToPostgres.TranslateInsertToPostgreSQL("TestTable", row);

        // Assert
        Assert.NotNull(result);
        Assert.Equal("INSERT INTO public.\"TestTable\" (\"Id\", \"Name\") VALUES ('1', 'Test') ON CONFLICT (\"Id\") DO NOTHING;", result);
    }

    [Fact]
    public void TestTranslateUpdateToPostgreSQL()
    {
        // Arrange
        var mockTable = new DataTable();
        mockTable.Columns.Add("Id", typeof(int));
        mockTable.Columns.Add("Name", typeof(string));
        mockTable.Columns.Add("__$operation", typeof(int));

        var row = mockTable.NewRow();
        row["Id"] = 1;
        row["Name"] = "Test";
        row["__$operation"] = 3;

        // Act
        var result = CDCTranslaterToPostgres.TranslateUpdateToPostgreSQL("TestTable", row);

        // Assert
        Assert.NotNull(result);
        Assert.Equal("UPDATE public.\"TestTable\" SET \"Id\" = '1', \"Name\" = 'Test' WHERE \"Id\" = '1';", result);
    }

    [Fact]
    public void TestTranslateDeleteToPostgreSQL()
    {
        // Arrange
        var mockTable = new DataTable();
        mockTable.Columns.Add("Id", typeof(int));
        mockTable.Columns.Add("__$operation", typeof(int));

        var row = mockTable.NewRow();
        row["Id"] = 1;
        row["__$operation"] = 1;

        // Act
        var result = CDCTranslaterToPostgres.TranslateDeleteToPostgreSQL("TestTable", row);

        // Assert
        Assert.NotNull(result);
        Assert.Equal("DELETE FROM public.\"TestTable\" WHERE \"Id\" = '1';", result);
    }
}