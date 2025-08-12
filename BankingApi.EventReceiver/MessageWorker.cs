using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using BankingApi.EventReceiver.DTO;

namespace BankingApi.EventReceiver
{
    public class MessageWorker : IHostedService, IAsyncDisposable
    {
        private readonly ServiceBusClient _sbClient;
        private ServiceBusProcessor? _processor;
        private readonly IServiceProvider _services;
        private readonly ILogger<MessageWorker> _log;
        private readonly IConfiguration _cfg;
        private readonly TimeSpan[] _retryBackoffs = new[] { TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(25), TimeSpan.FromSeconds(125) };

        public MessageWorker(
            ServiceBusClient sbClient,
            IServiceProvider services,
            ILogger<MessageWorker> logger,
            IConfiguration cfg)
        {
            _sbClient = sbClient ?? throw new ArgumentNullException(nameof(sbClient));
            _services = services ?? throw new ArgumentNullException(nameof(services));
            _log = logger ?? throw new ArgumentNullException(nameof(logger));
            _cfg = cfg ?? throw new ArgumentNullException(nameof(cfg));
        }

        public Task StartAsync(CancellationToken cancellationToken) => Start(cancellationToken);

        /// <summary>
        /// Start listening to Service Bus queue with PeekLock semantics.
        /// </summary>
        public async Task Start(CancellationToken cancellationToken = default)
        {
            var queueName = _cfg["ServiceBus:QueueName"] ?? throw new InvalidOperationException("ServiceBus:QueueName not configured");

            var processorOptions = new ServiceBusProcessorOptions
            {
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                AutoCompleteMessages = false,
                MaxConcurrentCalls = int.TryParse(_cfg["ServiceBus:MaxConcurrentCalls"], out var m) ? Math.Max(1, m) : 8
            };

            _processor = _sbClient.CreateProcessor(queueName, processorOptions);

            _processor.ProcessMessageAsync += ProcessMessageHandlerAsync;
            _processor.ProcessErrorAsync += ProcessErrorHandlerAsync;

            _log.LogInformation("Starting Service Bus processor for queue '{QueueName}', MaxConcurrentCalls={Max}", queueName, processorOptions.MaxConcurrentCalls);
            await _processor.StartProcessingAsync(cancellationToken);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            if (_processor != null)
            {
                _log.LogInformation("Stopping Service Bus processor...");
                await _processor.StopProcessingAsync(cancellationToken);
                _processor.ProcessMessageAsync -= ProcessMessageHandlerAsync;
                _processor.ProcessErrorAsync -= ProcessErrorHandlerAsync;
                await _processor.DisposeAsync();
                _processor = null;
            }
        }

        private Task ProcessErrorHandlerAsync(ProcessErrorEventArgs args)
        {
            // top-level errors (connectivity, permissions etc)
            _log.LogError(args.Exception, "ServiceBus error: Operation={Operation}, EntityPath={EntityPath}, ErrorSource={ErrorSource}",
                args.ErrorSource, args.EntityPath, args.Exception.Message);
            return Task.CompletedTask;
        }

        private async Task ProcessMessageHandlerAsync(ProcessMessageEventArgs args)
        {
            var message = args.Message;
            var ct = args.CancellationToken;
            string body = message.Body.ToString();

            TransactionMessageDto? dto;
            try
            {
                dto = JsonSerializer.Deserialize<TransactionMessageDto>(body, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                if (dto == null) throw new InvalidOperationException("Deserialized DTO is null");
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "Invalid JSON payload. Dead-lettering MessageId={MsgId}", message.MessageId);
                await args.DeadLetterMessageAsync(message, "InvalidPayload", ex.Message, ct);
                return;
            }

            // Idempotency check + processing in a scoped DbContext
            try
            {
                using var scope = _services.CreateScope();
                var dbFactory = scope.ServiceProvider.GetRequiredService<IDbContextFactory<BankingApiDbContext>>();
                // create a fresh context for this message
                await using var db = await dbFactory.CreateDbContextAsync(ct);

                // quick idempotency check
                var already = await db.ProcessedMessages.AsNoTracking().AnyAsync(p => p.MessageId == dto.Id, ct);
                if (already)
                {
                    _log.LogInformation("Message already processed, completing. MessageId={MsgId}", dto.Id);
                    await args.CompleteMessageAsync(message, ct);
                    return;
                }

                // compute delta: Credit -> +amount, Debit -> -amount
                var delta = dto.MessageType.Equals("Credit", StringComparison.OrdinalIgnoreCase) ? dto.Amount :
                            dto.MessageType.Equals("Debit", StringComparison.OrdinalIgnoreCase) ? -dto.Amount :
                            throw new InvalidOperationException($"Unsupported MessageType '{dto.MessageType}'");

                // Wrap DB updates in transaction to ensure atomicity
                await using var tx = await db.Database.BeginTransactionAsync(ct);

                // Use a single SQL UPDATE to avoid a read-modify-write race
                int rows;
                if (delta < 0)
                {
                    // Only update if balance remains >= 0 (business rule: no overdraft)
                    rows = await db.Database.ExecuteSqlInterpolatedAsync(
                        $"UPDATE BankAccounts SET Balance = Balance + {delta} WHERE Id = {dto.BankAccountId} AND Balance + {delta} >= 0",
                        ct);
                    if (rows == 0)
                    {
                        // determine cause: missing account or insufficient funds
                        var exists = await db.BankAccounts.AsNoTracking().AnyAsync(b => b.Id == dto.BankAccountId, ct);
                        if (!exists)
                        {
                            _log.LogWarning("Account not found. Dead-lettering. AccountId={AccountId} MessageId={MsgId}", dto.BankAccountId, dto.Id);
                            await args.DeadLetterMessageAsync(message, "AccountNotFound", $"BankAccount {dto.BankAccountId} not found", ct);
                            return;
                        }
                        else
                        {
                            _log.LogWarning("Insufficient funds. Dead-lettering. AccountId={AccountId} MessageId={MsgId}", dto.BankAccountId, dto.Id);
                            await args.DeadLetterMessageAsync(message, "InsufficientFunds", "Insufficient balance for debit", ct);
                            return;
                        }
                    }
                }
                else
                {
                    rows = await db.Database.ExecuteSqlInterpolatedAsync(
                        $"UPDATE BankAccounts SET Balance = Balance + {delta} WHERE Id = {dto.BankAccountId}",
                        ct);
                    if (rows == 0)
                    {
                        _log.LogWarning("Account not found (credit). Dead-lettering. AccountId={AccountId} MessageId={MsgId}", dto.BankAccountId, dto.Id);
                        await args.DeadLetterMessageAsync(message, "AccountNotFound", $"BankAccount {dto.BankAccountId} not found", ct);
                        return;
                    }
                }

                // record processed message + audit
                db.ProcessedMessages.Add(new ProcessedMessage { MessageId = dto.Id, ProcessedAt = DateTime.UtcNow });
                db.TransactionAudits.Add(new TransactionAudit { Id = Guid.NewGuid(), MessageId = dto.Id, BankAccountId = dto.BankAccountId, Delta = delta, CreatedAt = DateTime.UtcNow });

                await db.SaveChangesAsync(ct);
                await tx.CommitAsync(ct);

                _log.LogInformation("Processed message successfully. MessageId={MsgId} AccountId={AccountId} Delta={Delta}", dto.Id, dto.BankAccountId, delta);

                await args.CompleteMessageAsync(message, ct);
                return;
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                // Transient error -> schedule retry with backoff (so delivery count doesn't grow)
                try
                {
                    int attempt = 0;
                    if (message.ApplicationProperties.TryGetValue("retryAttempt", out var att) && att is int ai) attempt = ai;

                    if (attempt < _retryBackoffs.Length)
                    {
                        var nextAttempt = attempt + 1;
                        var delay = _retryBackoffs[attempt];
                        var scheduledEnqueue = DateTimeOffset.UtcNow.Add(delay);

                        // create sender scoped from the client
                        var queueName = _cfg["ServiceBus:QueueName"] ?? throw new InvalidOperationException("ServiceBus:QueueName not configured");
                        var sender = _sbClient.CreateSender(queueName);

                        var scheduledMessage = new ServiceBusMessage(message.Body)
                        {
                            ContentType = message.ContentType,
                            CorrelationId = message.CorrelationId,
                            Subject = message.Subject,
                            MessageId = dto.Id // preserve id for idempotency
                        };

                        // copy properties and bump retryAttempt
                        foreach (var kv in message.ApplicationProperties)
                            scheduledMessage.ApplicationProperties[kv.Key] = kv.Value;
                        scheduledMessage.ApplicationProperties["retryAttempt"] = nextAttempt;

                        await sender.ScheduleMessageAsync(scheduledMessage, scheduledEnqueue, ct);

                        _log.LogWarning(ex, "Transient failure. Scheduled retryAttempt={Attempt} in {Delay}s for MessageId={MsgId}", nextAttempt, delay.TotalSeconds, dto.Id);

                        // Complete original message (we scheduled a replacement)
                        await args.CompleteMessageAsync(message, ct);
                        return;
                    }
                    else
                    {
                        _log.LogError(ex, "Exceeded retry attempts. Dead-lettering MessageId={MsgId}", dto.Id);
                        await args.DeadLetterMessageAsync(message, "RetriesExceeded", ex.Message, ct);
                        return;
                    }
                }
                catch (Exception scheduleEx)
                {
                    _log.LogError(scheduleEx, "Failed to schedule retry for MessageId={MsgId}. Abandoning original message to let SB redeliver.", dto.Id);
                    // Abandon so Service Bus delivery count increments and default retry/DLQ behaviour applies
                    await args.AbandonMessageAsync(message, cancellationToken: ct);
                    return;
                }
            }
            catch (InvalidOperationException inv)
            {
                // Business error: dead-letter
                _log.LogWarning(inv, "Business error for MessageId={MsgId}. Dead-lettering.", dto.Id);
                await args.DeadLetterMessageAsync(message, "BusinessError", inv.Message, ct);
                return;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Unexpected error processing MessageId={MsgId}. Abandoning.", dto.Id);
                try { await args.AbandonMessageAsync(message, cancellationToken: ct); } catch { }
                return;
            }
        }

        private static bool IsTransient(Exception ex)
        {
            if (ex is ServiceBusException sb && sb.IsTransient) return true;
            if (ex is TimeoutException) return true;
            if (ex is OperationCanceledException) return true;
            // Add SQL deadlock detection (Microsoft.Data.SqlClient.SqlException.Number == 1205) if you reference that package
            return false;
        }

        public async ValueTask DisposeAsync()
        {
            if (_processor != null)
            {
                await _processor.StopProcessingAsync();
                await _processor.DisposeAsync();
                _processor = null;
            }
            await _sbClient.DisposeAsync();
        }
    }
}
