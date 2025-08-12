using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BankingApi.EventReceiver
{
    public class TransactionAudit
    {
        public Guid Id { get; set; }
        public string MessageId { get; set; } = null!;
        public Guid BankAccountId { get; set; }
        public decimal Delta { get; set; }
        public DateTime CreatedAt { get; set; }
    }
}
