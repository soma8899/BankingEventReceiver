using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace BankingApi.EventReceiver
{
    public class BankAccountService
    {
        public async Task<(bool Success, string? Error)> UpdateBalanceAsync(BankingApiDbContext db, Guid bankAccountId, decimal delta, CancellationToken ct)
        {
            int rowsAffected;
            if (delta < 0)
            {
                rowsAffected = await db.Database.ExecuteSqlInterpolatedAsync($"UPDATE BankAccounts SET Balance = Balance + {delta} WHERE Id = {bankAccountId} AND Balance + {delta} >= 0", ct).ConfigureAwait(false);
                if (rowsAffected == 0)
                {
                    var exists = await db.BankAccounts.AsNoTracking().AnyAsync(b => b.Id == bankAccountId, ct).ConfigureAwait(false);
                    if (!exists)
                        return (false, "AccountNotFound");
                    return (false, "InsufficientFunds");
                }
            }
            else
            {
                rowsAffected = await db.Database.ExecuteSqlInterpolatedAsync($"UPDATE BankAccounts SET Balance = Balance + {delta} WHERE Id = {bankAccountId}", ct).ConfigureAwait(false);
                if (rowsAffected == 0)
                    return (false, "AccountNotFound");
            }
            return (true, null);
        }
    }
}
