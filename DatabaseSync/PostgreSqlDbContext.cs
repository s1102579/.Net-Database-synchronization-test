using DatabaseSync.Entities;
using Microsoft.EntityFrameworkCore;

public class PostgreSqlDbContext : DbContext
{
    public DbSet<Log> Logs { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Log>().ToTable("dbo.Logs", schema: "public");
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseNpgsql("Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;Database=postgres_sync_database;TrustServerCertificate=True;");
    }
}