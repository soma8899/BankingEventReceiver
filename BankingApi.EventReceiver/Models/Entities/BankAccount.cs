using System;

namespace BankingApi.EventReceiver.Models.Entities
{
    // Represents a bank account entity in the system.
    public class BankAccount
    {
        public Guid Id { get; set; }
        public decimal Balance { get; set; }
    }
}
