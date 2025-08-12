using System;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Azure.Messaging.ServiceBus;

namespace BankingApi.EventReceiver
{
    public static class ErrorUtils
    {
        public static bool IsTransient(Exception ex)
        {
            if (ex is ServiceBusException sbe && sbe.IsTransient) return true;
            if (ex is TimeoutException) return true;
            if (ex is OperationCanceledException) return true;
            // Add SQL transient checks here if you want (deadlocks/timeouts).
            return false;
        }

        public static bool IsDuplicateKey(DbUpdateException ex)
        {
            for (var e = ex.InnerException; e != null; e = e.InnerException)
                if (e is SqlException sql && (sql.Number == 2627 || sql.Number == 2601)) return true;
            return ex.InnerException?.Message.Contains("duplicate key", StringComparison.OrdinalIgnoreCase) == true;
        }
    }
}
