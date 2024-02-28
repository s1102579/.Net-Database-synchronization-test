using DatabaseSync.Entities;
using Microsoft.EntityFrameworkCore;

public class SqlServerDbContext : DbContext
{
    public DbSet<Log> Logs { get; set; }

    // Singleton pattern not neccesary in .net api application you will add the context to the services and it will be a singleton
    private static SqlServerDbContext? _instance;

    public static SqlServerDbContext Instance
    {
        get
        {
            if (_instance == null)
            {
                var optionsBuilder = new DbContextOptionsBuilder<SqlServerDbContext>();
                optionsBuilder.UseSqlServer("Server=localhost,1433;Database=MSSQL_LOG_TEST;User Id=sa;Password=Your_Strong_Password;TrustServerCertificate=True;");
                _instance = new SqlServerDbContext(optionsBuilder.Options);
            }
            return _instance;
        }
    }

    private SqlServerDbContext(DbContextOptions<SqlServerDbContext> options) : base(options) { }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseSqlServer("Server=localhost,1433;Database=MSSQL_LOG_TEST;User Id=sa;Password=Your_Strong_Password;TrustServerCertificate=True;");
    }
}