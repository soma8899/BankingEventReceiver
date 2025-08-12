using Microsoft.EntityFrameworkCore;

namespace BankingApi.EventReceiver
{
    public class BankingApiDbContext : DbContext
    {
        public DbSet<BankAccount> BankAccounts { get; set; }

        public DbSet<ProcessedMessage> ProcessedMessages { get; set; }

        public DbSet<TransactionAudit> TransactionAudits { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder options)
            => options.UseSqlServer("Data Source=.\\SQLEXPRESS;Initial Catalog=BankingApiTest;Integrated Security=True;TrustServerCertificate=True;");
    }
}
