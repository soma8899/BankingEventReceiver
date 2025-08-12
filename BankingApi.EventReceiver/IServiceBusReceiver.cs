using BankingApi.EventReceiver.Models;

namespace BankingApi.EventReceiver
{
    public interface IServiceBusReceiver
    {
        Task<EventMessage?> Peek();

        Task Abandon(EventMessage message);
        
        Task Complete(EventMessage message);
        Task ReSchedule(EventMessage message, DateTime nextAvailableTime);
        Task MoveToDeadLetter(EventMessage message);
    }
}
