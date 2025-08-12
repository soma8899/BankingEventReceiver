using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BankingApi.EventReceiver
{
    public class ProcessedMessage
    {
        public string MessageId { get; set; } = null!;
        public DateTime ProcessedAt { get; set; }
    }
}
