using System;

namespace BankingApi.EventReceiver.Models
{
    // Tracks processed messages for idempotency.
    public class ProcessedMessage
    {
        public string MessageId { get; set; } = null!;
        public DateTime ProcessedAt { get; set; }
    }
}
