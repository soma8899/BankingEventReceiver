using System;

namespace BankingApi.EventReceiver.Models
{
    // Represents an audit record for a transaction.
    public class TransactionAudit
    {
        public Guid Id { get; set; }
        public string MessageId { get; set; } = null!;
        public Guid BankAccountId { get; set; }
        public decimal Delta { get; set; }
        public DateTime CreatedAt { get; set; }
    }
}
