using DatabaseSync.Entities;
using Microsoft.EntityFrameworkCore;

public class MySQLDbContext : DbContext
{
    public DbSet<Log> Logs { get; set; }
    public DbSet<AuditLog> AuditLogs { get; set; }

    // Singleton pattern not necessary in .net api application you will add the context to the services and it will be a singleton
    private static MySQLDbContext? _instance;

    public static MySQLDbContext Instance
    {
        get
        {
            if (_instance == null)
            {
                var optionsBuilder = new DbContextOptionsBuilder<MySQLDbContext>();
                optionsBuilder.UseMySql("Server=localhost;Database=My_SQL_SPEED_TEST;User Id=root;Password=Your_Strong_Password;AllowLoadLocalInfile=true;", new MySqlServerVersion(new Version(8, 0, 32)));
                _instance = new MySQLDbContext(optionsBuilder.Options);
            }
            return _instance;
        }
    }

    private MySQLDbContext(DbContextOptions<MySQLDbContext> options) : base(options) { }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseMySql("Server=localhost;Database=My_SQL_SPEED_TEST;User Id=root;Password=Your_Strong_Password;AllowLoadLocalInfile=true;", new MySqlServerVersion(new Version(8, 0, 32)));
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AuditLog>().HasNoKey();
    }
}