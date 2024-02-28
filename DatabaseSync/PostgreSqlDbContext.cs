using DatabaseSync.Entities;
using Microsoft.EntityFrameworkCore;

public class PostgreSqlDbContext : DbContext
{
    public DbSet<Log> Logs { get; set; }

    private static PostgreSqlDbContext? _instance;

    public static PostgreSqlDbContext Instance
    {
        get
        {
            if (_instance == null)
            {
                var optionsBuilder = new DbContextOptionsBuilder<PostgreSqlDbContext>();
                optionsBuilder.UseNpgsql("Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;Database=postgres_sync_database;TrustServerCertificate=True;");
                _instance = new PostgreSqlDbContext(optionsBuilder.Options);
            }
            return _instance;
        }
    }

    private PostgreSqlDbContext(DbContextOptions<PostgreSqlDbContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Log>().ToTable("dbo.Logs", schema: "public");
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseNpgsql("Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;Database=postgres_sync_database;TrustServerCertificate=True;");
    }
}