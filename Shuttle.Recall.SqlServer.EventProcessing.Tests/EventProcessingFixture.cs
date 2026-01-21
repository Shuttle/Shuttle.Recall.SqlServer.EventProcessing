using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using Shuttle.Core.Pipelines.Logging;
using Shuttle.Recall.SqlServer.Storage;
using Shuttle.Recall.Testing;
using System.Diagnostics.CodeAnalysis;

namespace Shuttle.Recall.SqlServer.EventProcessing.Tests;

[SuppressMessage("Security", "EF1002:Risk of vulnerability to SQL injection", Justification = "Schema and table names are from trusted configuration sources")]
public class EventProcessingFixture : RecallFixture
{
    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_process_events_async(bool isTransactional)
    {
        var configuration = new ConfigurationBuilder()
            .AddUserSecrets<EventProcessingFixture>()
            .Build();

        var services = new ServiceCollection()
            .AddSingleton<IConfiguration>(configuration)
            .AddPipelineLogging();

        var fixtureOptions = new RecallFixtureOptions(services)
            .WithAddRecall(recallBuilder =>
            {
                recallBuilder.Options.EventStore.PrimitiveEventSequencerIdleDurations = [TimeSpan.FromMilliseconds(250)];
                recallBuilder.Options.EventProcessing.ProjectionProcessorIdleDurations = [TimeSpan.FromMilliseconds(250)];

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
            .WithStarting(StartingAsync)
            .WithEventProcessingHandlerTimeout(TimeSpan.FromSeconds(15));

        await ExerciseEventProcessingAsync(fixtureOptions, isTransactional);
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_process_events_with_delay_async(bool isTransactional)
    {
        var configuration = new ConfigurationBuilder()
            .AddUserSecrets<EventProcessingFixture>()
            .Build();

        var services = new ServiceCollection()
            .AddSingleton<IConfiguration>(configuration)
            .AddPipelineLogging();

        var fixtureOptions = new RecallFixtureOptions(services)
            .WithAddRecall(recallBuilder =>
            {
                recallBuilder.Options.EventStore.PrimitiveEventSequencerIdleDurations = [TimeSpan.FromMilliseconds(250)];
                recallBuilder.Options.EventProcessing.ProjectionProcessorIdleDurations = [TimeSpan.FromMilliseconds(250)];

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
            .WithStarting(StartingAsync);

        await ExerciseEventProcessingWithDelayAsync(fixtureOptions, isTransactional);
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_exercise_event_processing_with_deferred_handling_async(bool isTransactional)
    {
        var configuration = new ConfigurationBuilder()
            .AddUserSecrets<EventProcessingFixture>()
            .Build();

        var services = new ServiceCollection()
            .AddSingleton<IConfiguration>(configuration)
            .AddPipelineLogging();

        var fixtureOptions = new RecallFixtureOptions(services)
            .WithAddRecall(recallBuilder =>
            {
                recallBuilder.Options.EventStore.PrimitiveEventSequencerIdleDurations = [TimeSpan.FromMilliseconds(250)];
                recallBuilder.Options.EventProcessing.ProjectionProcessorIdleDurations = [TimeSpan.FromMilliseconds(250)];

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
            .WithStarting(StartingAsync)
            .WithEventProcessingHandlerTimeout(TimeSpan.FromMinutes(5));

        await ExerciseEventProcessingWithDeferredHandlingAsync(fixtureOptions, isTransactional);
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_process_events_with_failure_async(bool isTransactional)
    {
        var configuration = new ConfigurationBuilder()
            .AddUserSecrets<EventProcessingFixture>()
            .Build();

        var services = new ServiceCollection()
            .AddSingleton<IConfiguration>(configuration)
            .AddPipelineLogging();

        var fixtureOptions = new RecallFixtureOptions(services)
            .WithAddRecall(recallBuilder =>
            {
                recallBuilder.Options.EventStore.PrimitiveEventSequencerIdleDurations = [TimeSpan.FromMilliseconds(250)];
                recallBuilder.Options.EventProcessing.ProjectionProcessorIdleDurations = [TimeSpan.FromMilliseconds(250)];

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
            .WithStarting(StartingAsync);

        await ExerciseEventProcessingWithFailureAsync(fixtureOptions, isTransactional);
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_process_volume_events_async(bool isTransactional)
    {
        var configuration = new ConfigurationBuilder()
            .AddUserSecrets<EventProcessingFixture>()
            .Build();

        var services = new ServiceCollection()
            .AddSingleton<IConfiguration>(configuration)
            .AddPipelineLogging();

        var fixtureOptions = new RecallFixtureOptions(services)
            .WithStarting(StartingAsync)
            .WithAddRecall(recallBuilder =>
            {
                recallBuilder.Options.EventStore.PrimitiveEventSequencerIdleDurations = [TimeSpan.FromMilliseconds(250)];
                recallBuilder.Options.EventProcessing.ProjectionProcessorIdleDurations = [TimeSpan.FromMilliseconds(250)];
                recallBuilder.Options.EventProcessing.ProjectionThreadCount = 5;

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
            .WithEventProcessingHandlerTimeout(TimeSpan.FromMinutes(2));

        await ExerciseEventProcessingVolumeAsync(fixtureOptions, isTransactional);
    }

    private static async Task StartingAsync(IServiceProvider serviceProvider)
    {
        using var scope = serviceProvider.GetRequiredService<IServiceScopeFactory>().CreateScope();

        var sqlServerStorageOptions = serviceProvider.GetRequiredService<IOptions<SqlServerStorageOptions>>().Value;
        var sqlEventProcessingOptions = serviceProvider.GetRequiredService<IOptions<SqlServerEventProcessingOptions>>().Value;

        await using (var dbContext = scope.ServiceProvider.GetRequiredService<SqlServerStorageDbContext>())
        {
            await dbContext.Database.ExecuteSqlRawAsync(@$"
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{sqlServerStorageOptions.Schema}].[PrimitiveEvent]') AND type in (N'U'))
BEGIN
    DELETE FROM [{sqlServerStorageOptions.Schema}].[PrimitiveEvent]
    FROM
        [{sqlServerStorageOptions.Schema}].[PrimitiveEvent] pe
    INNER JOIN 
        [{sqlServerStorageOptions.Schema}].[EventType] et ON pe.EventTypeId = et.Id
    WHERE
        et.[TypeName] LIKE 'Shuttle.Recall.Testing%'
END
");
        }

        await using (var dbContext = scope.ServiceProvider.GetRequiredService<SqlServerEventProcessingDbContext>())
        {
            await dbContext.Database.ExecuteSqlRawAsync(@$"
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{sqlEventProcessingOptions.Schema}].[Projection]') AND type in (N'U'))
BEGIN
    DELETE FROM [{sqlEventProcessingOptions.Schema}].[Projection] WHERE [Name] like 'recall-fixture%'
END

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{sqlEventProcessingOptions.Schema}].[ProjectionJournal]') AND type in (N'U'))
BEGIN
    DELETE FROM [{sqlEventProcessingOptions.Schema}].[ProjectionJournal] WHERE [Name] like 'recall-fixture%'
END
");
        }
    }
}