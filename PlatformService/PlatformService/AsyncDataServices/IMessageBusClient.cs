using PlatformService.Dtos;

namespace PlatformService.AsyncDataServices
{
    public interface IMessageBusClient
    {
        Task PublishNewPlatform(PlatformPublishedDto platformPublishedDto);
    }
}
