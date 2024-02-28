using DatabaseSync.Entities;
using Microsoft.EntityFrameworkCore;

public class SqlServerDbContext : DbContext
{
    public DbSet<Log> Logs { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseSqlServer("Server=localhost,1433;Database=MSSQL_LOG_TEST;User Id=sa;Password=Your_Strong_Password;TrustServerCertificate=True;");
    }
}