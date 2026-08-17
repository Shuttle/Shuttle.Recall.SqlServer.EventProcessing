using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using Shuttle.Recall.SqlServer.Storage;
using Shuttle.Recall.Testing;

namespace Shuttle.Recall.SqlServer.EventProcessing.Tests;

public class ImmediateProjectionEventRepositoryFixture
{
    private static readonly Guid EventId = new("6E1C5B9B-6F0A-4B6E-9C8B-9A6F7E9F5A21");
    private const string ProjectionName = "immediate-projection-event-repository-fixture";

    [Test]
    public async Task Should_be_able_to_track_immediate_projection_events_async()
    {
        var configuration = new ConfigurationBuilder()
            .AddUserSecrets<ImmediateProjectionEventRepositoryFixture>()
            .Build();

        var serviceProvider = new ServiceCollection()
            .AddSingleton<IConfiguration>(configuration)
            .AddLogging()
            .AddRecall()
            .UseSqlServerEventStorage(options =>
            {
                options.ConnectionString = configuration.GetConnectionString("StorageConnection") ?? throw new ApplicationException("A 'ConnectionString' with name 'StorageConnection' is required which points to a Sql Server database that will contain the event storage.");
                options.Schema = "recall_fixture";
            })
            .UseSqlServerEventProcessing()
            .Services
            .BuildServiceProvider();

        await serviceProvider.StartHostedServicesAsync();

        var repository = serviceProvider.GetRequiredService<IImmediateProjectionEventRepository>();
        var options = serviceProvider.GetRequiredService<IOptions<SqlServerStorageOptions>>().Value;

        await using var dbContext = serviceProvider.GetRequiredService<SqlServerStorageDbContext>();

#pragma warning disable EF1002
        await dbContext.Database.ExecuteSqlRawAsync($"DELETE FROM [{options.Schema}].[ImmediateProjectionEvent] WHERE [ProjectionName] = @ProjectionName", new SqlParameter("@ProjectionName", ProjectionName));
#pragma warning restore EF1002

        Assert.That(await repository.ContainsAsync(ProjectionName, EventId), Is.False, "Should not contain the event before it has been saved.");

        await repository.SaveAsync(ProjectionName, EventId);

        Assert.That(await repository.ContainsAsync(ProjectionName, EventId), Is.True, "Should contain the event once it has been saved.");

        // Saving the same projection/event pair again should be idempotent (no primary key violation).
        await repository.SaveAsync(ProjectionName, EventId);

        Assert.That(await repository.ContainsAsync(ProjectionName, EventId), Is.True, "Should still contain the event after saving it again.");

        await repository.RemoveAsync(ProjectionName, EventId);

        Assert.That(await repository.ContainsAsync(ProjectionName, EventId), Is.False, "Should not contain the event once it has been removed.");

        // Removing an event that is not present should not throw.
        await repository.RemoveAsync(ProjectionName, EventId);
    }
}
