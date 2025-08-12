using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BankingApi.EventReceiver.DTO
{
    public class TransactionMessageDto
    {
        public string Id { get; set; } = null!;
        public string MessageType { get; set; } = null!;
        public Guid BankAccountId { get; set; }
        public decimal Amount { get; set; }
    }
}
