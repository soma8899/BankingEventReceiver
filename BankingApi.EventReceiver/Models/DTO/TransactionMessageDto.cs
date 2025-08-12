using System;

namespace BankingApi.EventReceiver.Models.DTO
{
    // Data Transfer Object for transaction messages received from the service bus.
    public class TransactionMessageDto
    {
        public string Id { get; set; } = null!;
        public string MessageType { get; set; } = null!;
        public Guid BankAccountId { get; set; }
        public decimal Amount { get; set; }
    }
}
