using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NUnit.Framework;
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
        var services = SqlServerFixtureConfiguration.GetServiceCollection();

        var fixtureConfiguration = new FixtureConfiguration(services)
            .WithStarting(StartingAsync)
            .WithAddEventStore(builder =>
            {
                builder.Options.ProjectionThreadCount = 1;
            })
            .WithHandlerTimeout(TimeSpan.FromMinutes(5));

        await ExerciseEventProcessingAsync(fixtureConfiguration, isTransactional);
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_process_events_with_delay_async(bool isTransactional)
    {
        var services = SqlServerFixtureConfiguration.GetServiceCollection();

        var fixtureConfiguration = new FixtureConfiguration(services)
            .WithStarting(StartingAsync)
            .WithAddEventStore(builder =>
            {
                builder.Options.ProjectionThreadCount = 1;
            })
            .WithHandlerTimeout(TimeSpan.FromMinutes(5));

        await ExerciseEventProcessingWithDelayAsync(fixtureConfiguration, isTransactional);
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_process_events_with_failure_async(bool isTransactional)
    {
        var services = SqlServerFixtureConfiguration.GetServiceCollection();

        var fixtureConfiguration = new FixtureConfiguration(services)
            .WithStarting(StartingAsync)
            .WithAddEventStore(builder =>
            {
                builder.Options.ProjectionThreadCount = 1;
            });

        await ExerciseEventProcessingWithFailureAsync(fixtureConfiguration, isTransactional);
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_process_volume_events_async(bool isTransactional)
    {
        var services = SqlServerFixtureConfiguration.GetServiceCollection();

        var fixtureConfiguration = new FixtureConfiguration(services)
            .WithStarting(StartingAsync)
            .WithAddEventStore(builder =>
            {
                builder.Options.ProjectionThreadCount = 25;
            })
            .WithHandlerTimeout(TimeSpan.FromMinutes(5));

        await ExerciseEventProcessingVolumeAsync(fixtureConfiguration, isTransactional);
    }

    private static async Task StartingAsync(IServiceProvider serviceProvider)
    {
        var sqlServerStorageOptions = serviceProvider.GetRequiredService<IOptions<SqlServerStorageOptions>>().Value;
        var sqlEventProcessingOptions = serviceProvider.GetRequiredService<IOptions<SqlServerEventProcessingOptions>>().Value;
        var sqlServerStorageDbContextFactory = serviceProvider.GetRequiredService<IDbContextFactory<SqlServerStorageDbContext>>();
        var sqlServerEventProcessingDbContext = serviceProvider.GetRequiredService<IDbContextFactory<SqlServerEventProcessingDbContext>>();

        await using (var dbContext = await sqlServerStorageDbContextFactory.CreateDbContextAsync())
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
        et.[TypeName] LIKE 'Shuttle.Recall.Tests%'
END
");
        }

        await using (var dbContext = await sqlServerEventProcessingDbContext.CreateDbContextAsync())
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