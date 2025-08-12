using BankingApi.EventReceiver.Models;
using BankingApi.EventReceiver.Models.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

namespace BankingApi.EventReceiver
{
    public class BankingApiDbContext : DbContext
    {
        public DbSet<BankAccount> BankAccounts { get; set; }

        public DbSet<ProcessedMessage> ProcessedMessages { get; set; }

        public DbSet<TransactionAudit> TransactionAudits { get; set; }

        private readonly IConfiguration _configuration;

        public BankingApiDbContext(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                var connStr = _configuration.GetConnectionString("DefaultConnection");
                optionsBuilder.UseSqlServer(connStr);
            }
        }
    }
}
