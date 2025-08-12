using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using BankingApi.EventReceiver.Models.DTO;

namespace BankingApi.EventReceiver
{
    /// <summary>
    /// Azure Service Bus worker with idempotent processing, scheduled retries, and EF Core unit of work.
    /// </summary>
    public sealed class MessageWorker : IHostedService, IAsyncDisposable
    {
        // One-time objects
        private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };
        private static readonly TimeSpan[] RetryBackoffs = { TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(25), TimeSpan.FromSeconds(125) };

        private readonly ServiceBusClient _sbClient;
        private readonly IServiceProvider _services;
        private readonly ILogger<MessageWorker> _log;
        private readonly IConfiguration _cfg;

        // Runtime
        private ServiceBusProcessor? _processor;
        private ServiceBusSender? _sender;
        private string? _queueName;

        private readonly IdempotencyService _idempotencyService = new();
        private readonly BankAccountService _bankAccountService = new();
        private readonly TransactionAuditService _auditService = new();

        public MessageWorker(ServiceBusClient sbClient, IServiceProvider services, ILogger<MessageWorker> logger, IConfiguration cfg)
        {
            _sbClient = sbClient ?? throw new ArgumentNullException(nameof(sbClient));
            _services = services ?? throw new ArgumentNullException(nameof(services));
            _log = logger ?? throw new ArgumentNullException(nameof(logger));
            _cfg = cfg ?? throw new ArgumentNullException(nameof(cfg));
        }

        public Task StartAsync(CancellationToken cancellationToken) => Start(cancellationToken);

        public async Task Start(CancellationToken cancellationToken = default)
        {
            _queueName = _cfg["ServiceBus:QueueName"] ?? throw new InvalidOperationException("ServiceBus:QueueName not configured");

            var maxConcurrent = int.TryParse(_cfg["ServiceBus:MaxConcurrentCalls"], out var mc) ? Math.Max(1, mc) : Math.Max(4, Environment.ProcessorCount);
            var prefetch = int.TryParse(_cfg["ServiceBus:PrefetchCount"], out var pf) ? Math.Max(0, pf) : 64;
            var autoRenew = TimeSpan.TryParse(_cfg["ServiceBus:MaxAutoLockRenewal"], out var ar) ? ar : TimeSpan.FromMinutes(5);

            var processorOptions = new ServiceBusProcessorOptions
            {
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                AutoCompleteMessages = false,
                MaxConcurrentCalls = maxConcurrent,
                PrefetchCount = prefetch,
                MaxAutoLockRenewalDuration = autoRenew
            };

            _processor = _sbClient.CreateProcessor(_queueName, processorOptions);
            _sender = _sbClient.CreateSender(_queueName);

            _processor.ProcessMessageAsync += ProcessMessageHandlerAsync;
            _processor.ProcessErrorAsync += ProcessErrorHandlerAsync;

            _log.LogInformation("SB processor starting: Queue={Queue}, Concurrency={Conc}, Prefetch={Prefetch}, AutoLockRenew={AutoRenew}",
                _queueName, maxConcurrent, prefetch, autoRenew);

            await _processor.StartProcessingAsync(cancellationToken).ConfigureAwait(false);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            if (_processor is null) return;

            _log.LogInformation("SB processor stopping...");
            try
            {
                await _processor.StopProcessingAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _processor.ProcessMessageAsync -= ProcessMessageHandlerAsync;
                _processor.ProcessErrorAsync -= ProcessErrorHandlerAsync;

                await _processor.DisposeAsync().ConfigureAwait(false);
                _processor = null;

                if (_sender is not null)
                {
                    await _sender.DisposeAsync().ConfigureAwait(false);
                    _sender = null;
                }
            }
        }

        private Task ProcessErrorHandlerAsync(ProcessErrorEventArgs args)
        {
            _log.LogError(args.Exception,
                "ServiceBus error: Source={Source}, Entity={Entity}, Namespace={Ns}",
                args.ErrorSource, args.EntityPath, args.FullyQualifiedNamespace);
            return Task.CompletedTask;
        }

        private async Task ProcessMessageHandlerAsync(ProcessMessageEventArgs args)
        {
            var msg = args.Message;
            var ct = args.CancellationToken;

            TransactionMessageDto dto;
            try
            {
                dto = JsonSerializer.Deserialize<TransactionMessageDto>(msg.Body, JsonOpts)
                      ?? throw new InvalidOperationException("Deserialized DTO is null.");
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "Bad JSON. Dead-lettering. MessageId={MsgId}", msg.MessageId);
                await args.DeadLetterMessageAsync(msg, "InvalidPayload", ex.Message, ct).ConfigureAwait(false);
                return;
            }

            try
            {
                using var scope = _services.CreateScope();
                var dbFactory = scope.ServiceProvider.GetRequiredService<IDbContextFactory<BankingApiDbContext>>();
                await using var db = await dbFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
                await using var tx = await db.Database.BeginTransactionAsync(ct).ConfigureAwait(false);

                // Idempotency check
                var idempotent = await _idempotencyService.TryInsertProcessedMarkerAsync(db, dto.Id, ct);
                if (!idempotent)
                {
                    _log.LogDebug("Duplicate message. Completing. MessageId={MsgId}", dto.Id);
                    await args.CompleteMessageAsync(msg, ct).ConfigureAwait(false);
                    return;
                }

                // Compute delta
                var delta = dto.MessageType.Equals("Credit", StringComparison.OrdinalIgnoreCase) ? dto.Amount
                          : dto.MessageType.Equals("Debit", StringComparison.OrdinalIgnoreCase) ? -dto.Amount
                          : throw new InvalidOperationException($"Unsupported MessageType '{dto.MessageType}'");

                // Balance update
                var (success, error) = await _bankAccountService.UpdateBalanceAsync(db, dto.BankAccountId, delta, ct);
                if (!success)
                {
                    await tx.RollbackAsync(ct).ConfigureAwait(false);
                    if (error == "AccountNotFound")
                        await args.DeadLetterMessageAsync(msg, "AccountNotFound", $"BankAccount {dto.BankAccountId} not found", ct).ConfigureAwait(false);
                    else if (error == "InsufficientFunds")
                        await args.DeadLetterMessageAsync(msg, "InsufficientFunds", "Insufficient balance for debit", ct).ConfigureAwait(false);
                    return;
                }

                // Audit
                db.TransactionAudits.Add(_auditService.CreateAudit(dto.Id, dto.BankAccountId, delta));
                await db.SaveChangesAsync(ct).ConfigureAwait(false);
                await tx.CommitAsync(ct).ConfigureAwait(false);

                _log.LogInformation("Processed MessageId={MsgId} AccountId={AccountId} Delta={Delta}", dto.Id, dto.BankAccountId, delta);
                await args.CompleteMessageAsync(msg, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ErrorUtils.IsTransient(ex))
            {
                var attempt = TryGetIntAppProp(msg, "retryAttempt") ?? 0;
                if (attempt < RetryBackoffs.Length)
                {
                    var delay = RetryBackoffs[attempt];
                    var next = attempt + 1;
                    try
                    {
                        await ScheduleRetryAsync(msg, next, delay, ct).ConfigureAwait(false);
                        _log.LogWarning(ex, "Transient error. Scheduled retry {Next} in {Delay}s for MessageId={MsgId}", next, delay.TotalSeconds, msg.MessageId);
                        await args.CompleteMessageAsync(msg, ct).ConfigureAwait(false);
                        return;
                    }
                    catch (Exception scheduleEx)
                    {
                        _log.LogError(scheduleEx, "Retry schedule failed for MessageId={MsgId}. Abandoning to let SB redeliver.", msg.MessageId);
                        await SafeAbandonAsync(args, msg, ct).ConfigureAwait(false);
                        return;
                    }
                }
                _log.LogError(ex, "Retries exhausted. Dead-lettering MessageId={MsgId}", msg.MessageId);
                await args.DeadLetterMessageAsync(msg, "RetriesExceeded", ex.Message, ct).ConfigureAwait(false);
            }
            catch (InvalidOperationException inv)
            {
                _log.LogWarning(inv, "Business error. Dead-lettering MessageId={MsgId}", msg.MessageId);
                await args.DeadLetterMessageAsync(msg, "BusinessError", inv.Message, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Unexpected error. Abandoning MessageId={MsgId}", msg.MessageId);
                await SafeAbandonAsync(args, msg, ct).ConfigureAwait(false);
            }
        }

        private async Task ScheduleRetryAsync(ServiceBusReceivedMessage original, int nextAttempt, TimeSpan delay, CancellationToken ct)
        {
            if (_sender is null) throw new InvalidOperationException("Service Bus sender not initialized.");

            var cloned = new ServiceBusMessage(original.Body)
            {
                ContentType = original.ContentType,
                CorrelationId = original.CorrelationId,
                Subject = original.Subject,
                MessageId = original.MessageId // keep for idempotency
            };

            foreach (var kv in original.ApplicationProperties)
                cloned.ApplicationProperties[kv.Key] = kv.Value;

            cloned.ApplicationProperties["retryAttempt"] = nextAttempt;

            await _sender.ScheduleMessageAsync(cloned, DateTimeOffset.UtcNow.Add(delay), ct).ConfigureAwait(false);
        }

        private static int? TryGetIntAppProp(ServiceBusReceivedMessage msg, string key) =>
            msg.ApplicationProperties.TryGetValue(key, out var v) && v is int i ? i : null;

        private static async Task SafeAbandonAsync(ProcessMessageEventArgs args, ServiceBusReceivedMessage msg, CancellationToken ct)
        {
            try { await args.AbandonMessageAsync(msg, cancellationToken: ct).ConfigureAwait(false); }
            catch { /* ignore */ }
        }

        public async ValueTask DisposeAsync()
        {
            await StopAsync(CancellationToken.None).ConfigureAwait(false);
            await _sbClient.DisposeAsync().ConfigureAwait(false);
        }
    }
}
