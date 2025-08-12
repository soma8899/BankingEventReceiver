using System;
using System.Threading;
using System.Threading.Tasks;
using BankingApi.EventReceiver.Models;
using Microsoft.EntityFrameworkCore;

namespace BankingApi.EventReceiver
{
    public class IdempotencyService
    {
        public async Task<bool> TryInsertProcessedMarkerAsync(BankingApiDbContext db, string messageId, CancellationToken ct)
        {
            db.ProcessedMessages.Add(new ProcessedMessage { MessageId = messageId, ProcessedAt = DateTime.UtcNow });
            try
            {
                await db.SaveChangesAsync(ct).ConfigureAwait(false);
                return true;
            }
            catch (DbUpdateException ex) when (ErrorUtils.IsDuplicateKey(ex))
            {
                return false;
            }
        }
    }
}
