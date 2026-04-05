using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using Shuttle.Recall.SqlServer.Storage;
using Shuttle.Recall.Testing;

namespace Shuttle.Recall.SqlServer.EventProcessing.Tests;

[SuppressMessage("Security", "EF1002:Risk of vulnerability to SQL injection", Justification = "Schema and table names are from trusted configuration sources")]
public class EventProcessingFixture : RecallFixture
{
    private static RecallFixtureOptions GetRecallFixtureOptions()
    {
        var configuration = new ConfigurationBuilder()
            .AddUserSecrets<EventProcessingFixture>()
            .Build();

        var services = new ServiceCollection()
            .AddSingleton<IConfiguration>(configuration)
            .AddKeyedScoped<DbConnection>("Testing", (serviceProvider, _) =>
                new SqlConnection(serviceProvider.GetRequiredService<IOptions<SqlServerStorageOptions>>().Value.ConnectionString))
            .AddRecall(options =>
            {
                options.EventStore.PrimitiveEventSequencerIdleDurations = [TimeSpan.FromMilliseconds(250)];
                options.EventProcessing.ProjectionProcessorIdleDurations = [TimeSpan.FromMilliseconds(250)];
            })
            .UseSqlServerEventStorage(options =>
            {
                options.ConnectionString = configuration.GetConnectionString("StorageConnection") ?? throw new ApplicationException("A 'ConnectionString' with name 'StorageConnection' is required which points to a Sql Server database that will contain the event storage.");
                options.Schema = "recall_fixture";
            })
            .RegisterPrimitiveEventSequencing()
            .UseSqlServerEventProcessing()
            .Services;

        return new RecallFixtureOptions(services)
            .WithStarting(StartingAsync);
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_exercise_event_processing_with_deferred_handling_async(bool isTransactional)
    {
        await ExerciseEventProcessingWithDeferredHandlingAsync(GetRecallFixtureOptions().WithEventProcessingHandlerTimeout(TimeSpan.FromMinutes(5)), isTransactional);
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_process_events_async(bool isTransactional)
    {
        await ExerciseEventProcessingAsync(GetRecallFixtureOptions().WithEventProcessingHandlerTimeout(TimeSpan.FromSeconds(1500)), isTransactional);
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_process_events_with_delay_async(bool isTransactional)
    {
        await ExerciseEventProcessingWithDelayAsync(GetRecallFixtureOptions(), isTransactional);
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_process_events_with_failure_async(bool isTransactional)
    {
        await ExerciseEventProcessingWithFailureAsync(GetRecallFixtureOptions(), isTransactional);
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_process_volume_events_async(bool isTransactional)
    {
        await ExerciseEventProcessingVolumeAsync(GetRecallFixtureOptions().WithEventProcessingHandlerTimeout(TimeSpan.FromMinutes(2)), isTransactional);
    }

    private static async Task StartingAsync(IServiceProvider serviceProvider)
    {
        using var scope = serviceProvider.GetRequiredService<IServiceScopeFactory>().CreateScope();

        var sqlServerStorageOptions = serviceProvider.GetRequiredService<IOptions<SqlServerStorageOptions>>().Value;

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
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{sqlServerStorageOptions.Schema}].[Projection]') AND type in (N'U'))
BEGIN
    DELETE FROM [{sqlServerStorageOptions.Schema}].[Projection] WHERE [Name] like 'recall-fixture%'
END

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{sqlServerStorageOptions.Schema}].[ProjectionJournal]') AND type in (N'U'))
BEGIN
    DELETE FROM [{sqlServerStorageOptions.Schema}].[ProjectionJournal] WHERE [Name] like 'recall-fixture%'
END
");
        }
    }
}