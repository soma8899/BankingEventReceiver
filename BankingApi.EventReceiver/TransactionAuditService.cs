using BankingApi.EventReceiver.Models;
using System;

namespace BankingApi.EventReceiver
{
    public class TransactionAuditService
    {
        public TransactionAudit CreateAudit(string messageId, Guid bankAccountId, decimal delta)
        {
            return new TransactionAudit
            {
                Id = Guid.NewGuid(),
                MessageId = messageId,
                BankAccountId = bankAccountId,
                Delta = delta,
                CreatedAt = DateTime.UtcNow
            };
        }
    }
}
