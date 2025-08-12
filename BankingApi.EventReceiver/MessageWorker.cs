namespace BankingApi.EventReceiver
{
    public class MessageWorker
    {
        public MessageWorker(IServiceBusReceiver serviceBusReceiver)
        {
        }

        public Task Start()
        {
            // Implement logic to listen to messages here placed
            return Task.CompletedTask;
        }
    }
}
