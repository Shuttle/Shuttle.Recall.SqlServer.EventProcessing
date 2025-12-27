using Microsoft.Extensions.DependencyInjection;
using Shuttle.Core.Contract;

namespace Shuttle.Recall.SqlServer.EventProcessing;

public class SqlServerEventProcessingBuilder(IServiceCollection services)
{
    public SqlServerEventProcessingOptions Options
    {
        get;
        set => field = Guard.AgainstNull(value);
    } = new();

    public IServiceCollection Services { get; } = Guard.AgainstNull(services);
}