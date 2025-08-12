using System;

namespace BankingApi.EventReceiver.Models
{
    // Defines the structure of an event message in the system.
    public class EventMessage
    {
        public Guid Id { get; set; }
        public string? MessageBody { get; set; }
        public int ProcessingCount { get; set; }
    }
}
