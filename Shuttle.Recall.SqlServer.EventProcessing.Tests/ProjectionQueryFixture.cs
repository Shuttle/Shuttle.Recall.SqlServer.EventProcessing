using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using Shuttle.Recall.SqlServer.Storage;
using Shuttle.Recall.Testing;

namespace Shuttle.Recall.SqlServer.EventProcessing.Tests;

public class ProjectionQueryFixture
{
    private const string DeferredProjectionName = "projection-query-fixture-deferred";
    private const string ProgressingProjectionName = "projection-query-fixture-progressing";
    private const string CaughtUpProjectionName = "projection-query-fixture-caught-up";

    [Test]
    public async Task Should_claim_a_progressing_projection_before_a_lower_previously_deferred_projection_async()
    {
        var configuration = new ConfigurationBuilder()
            .AddUserSecrets<ProjectionQueryFixture>()
            .Build();

        var serviceProvider = new ServiceCollection()
            .AddSingleton<IConfiguration>(configuration)
            .AddLogging()
            .AddRecall(options =>
            {
                // only the fixture's projections are eligible, regardless of what else is in the schema
                options.EventProcessing.IncludedProjections.Add(DeferredProjectionName);
                options.EventProcessing.IncludedProjections.Add(ProgressingProjectionName);
                options.EventProcessing.IncludedProjections.Add(CaughtUpProjectionName);
            })
            .UseSqlServerEventStorage(options =>
            {
                options.ConnectionString = configuration.GetConnectionString("StorageConnection") ?? throw new ApplicationException("A 'ConnectionString' with name 'StorageConnection' is required which points to a Sql Server database that will contain the event storage.");
                options.Schema = "recall_fixture";
            })
            .UseSqlServerEventProcessing()
            .Services
            .BuildServiceProvider();

        await serviceProvider.StartHostedServicesAsync();

        var options = serviceProvider.GetRequiredService<IOptions<SqlServerStorageOptions>>().Value;

        await using var dbContext = serviceProvider.GetRequiredService<SqlServerEventProcessingDbContext>();

        async Task ResetAsync()
        {
#pragma warning disable EF1002
            await dbContext.Database.ExecuteSqlRawAsync($@"
DELETE FROM [{options.Schema}].[Projection] WHERE [Name] IN (@DeferredProjectionName, @ProgressingProjectionName, @CaughtUpProjectionName);

DECLARE @MaximumSequenceNumber BIGINT = ISNULL((SELECT MAX([SequenceNumber]) FROM [{options.Schema}].[PrimitiveEvent]), 0);

-- the lower projection deferred previously (its deferral has elapsed), the higher one is progressing, and the last
-- has caught up (no further events); negative sequence numbers keep the first two behind whatever events exist
INSERT INTO [{options.Schema}].[Projection] ([Name], [SequenceNumber], [DeferredUntil], [FailureCount])
VALUES
    (@DeferredProjectionName, -2, DATEADD(SECOND, -1, SYSDATETIMEOFFSET()), 0),
    (@ProgressingProjectionName, -1, NULL, 0),
    (@CaughtUpProjectionName, @MaximumSequenceNumber, NULL, 0);
",
                new SqlParameter("@DeferredProjectionName", DeferredProjectionName),
                new SqlParameter("@ProgressingProjectionName", ProgressingProjectionName),
                new SqlParameter("@CaughtUpProjectionName", CaughtUpProjectionName));
#pragma warning restore EF1002
        }

        await ResetAsync();

        try
        {
            var projectionQuery = serviceProvider.GetRequiredService<IProjectionQuery>();

            // claims are made in a (read committed) transaction, as in `SequentialProjectionEventService.RetrieveAsync`
            await using (var transaction = await dbContext.Database.BeginTransactionAsync())
            {
                var first = await projectionQuery.GetPendingAsync();

                Assert.That(first?.Name, Is.EqualTo(ProgressingProjectionName), "The progressing projection should be claimed before the lower, previously deferred projection.");

                await transaction.CommitAsync();
            }

            // once nothing else is eligible (the first is now locked) the previously deferred projection still gets its turn
            await using (var transaction = await dbContext.Database.BeginTransactionAsync())
            {
                var second = await projectionQuery.GetPendingAsync();

                Assert.That(second?.Name, Is.EqualTo(DeferredProjectionName), "The previously deferred projection should be claimed when it is the only eligible projection.");

                await transaction.CommitAsync();
            }

            // the caught-up projection has nothing to process, so it is never claimed
            await using (var transaction = await dbContext.Database.BeginTransactionAsync())
            {
                Assert.That(await projectionQuery.GetPendingAsync(), Is.Null, "A projection that has caught up should not be claimed.");

                await transaction.CommitAsync();
            }
        }
        finally
        {
#pragma warning disable EF1002
            await dbContext.Database.ExecuteSqlRawAsync($"DELETE FROM [{options.Schema}].[Projection] WHERE [Name] IN (@DeferredProjectionName, @ProgressingProjectionName, @CaughtUpProjectionName)",
                new SqlParameter("@DeferredProjectionName", DeferredProjectionName),
                new SqlParameter("@ProgressingProjectionName", ProgressingProjectionName),
                new SqlParameter("@CaughtUpProjectionName", CaughtUpProjectionName));
#pragma warning restore EF1002
        }
    }
}
