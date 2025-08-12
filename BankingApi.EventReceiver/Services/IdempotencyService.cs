using Microsoft.EntityFrameworkCore;
using BankingApi.EventReceiver.Models;

namespace BankingApi.EventReceiver.Services
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
