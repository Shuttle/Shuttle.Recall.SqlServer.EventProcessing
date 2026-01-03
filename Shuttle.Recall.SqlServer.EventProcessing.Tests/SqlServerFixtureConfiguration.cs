using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Shuttle.Core.Pipelines.Logging;
using Shuttle.Recall.SqlServer.Storage;

namespace Shuttle.Recall.SqlServer.EventProcessing.Tests;

[SetUpFixture]
public class SqlServerFixtureConfiguration
{
    public static IServiceCollection GetServiceCollection(IServiceCollection? serviceCollection = null)
    {
        var configuration = new ConfigurationBuilder()
            .AddUserSecrets<SqlServerFixtureConfiguration>()
            .Build();

        var services = (serviceCollection ?? new ServiceCollection())
            .AddSingleton<IConfiguration>(configuration)
            .AddRecall(recallBuilder =>
            {
                recallBuilder
                    .UseSqlServerEventStorage(builder =>
                    {
                        builder.Options.ConnectionString = configuration.GetConnectionString("StorageConnection") ?? throw new ApplicationException("A 'ConnectionString' with name 'StorageConnection' is required which points to a Sql Server database that will contain the event storage.");
                        builder.Options.Schema = "recall_fixture";
                    })
                    .UseSqlServerEventProcessing(builder =>
                    {
                        builder.Options.ConnectionString = configuration.GetConnectionString("EventProcessingConnection") ?? throw new ApplicationException("A 'ConnectionString' with name 'EventProcessingConnection' is required which points to a Sql Server database that will contain the projections.");
                        builder.Options.Schema = "recall_fixture";
                    });
            })
            .AddPipelineLogging(); ;

        return services;
    }
}