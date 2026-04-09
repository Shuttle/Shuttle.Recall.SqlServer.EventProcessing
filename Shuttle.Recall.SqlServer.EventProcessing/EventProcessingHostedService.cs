using System.Data;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Shuttle.Reflection;
using Shuttle.Recall.SqlServer.Storage;

namespace Shuttle.Recall.SqlServer.EventProcessing;

[SuppressMessage("Security", "EF1002:Risk of vulnerability to SQL injection", Justification = "Schema and table names are from trusted configuration sources")]
public class EventProcessingHostedService(IOptions<RecallOptions> recallOptions, IOptions<SqlServerStorageOptions> sqlServerStorageOptions, IServiceScopeFactory serviceScopeFactory, IEventProcessorConfiguration eventProcessorConfiguration)
    : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(recallOptions);
        ArgumentNullException.ThrowIfNull(sqlServerStorageOptions);
        ArgumentNullException.ThrowIfNull(eventProcessorConfiguration);
        ArgumentNullException.ThrowIfNull(serviceScopeFactory);

        var schema = sqlServerStorageOptions.Value.Schema;

        using var scope = serviceScopeFactory.CreateScope();
        await using var dbContext = scope.ServiceProvider.GetRequiredService<SqlServerEventProcessingDbContext>();
        
        if (sqlServerStorageOptions.Value.ConfigureDatabase)
        {
            var retry = true;
            var retryCount = 0;

            while (retry)
            {
                await recallOptions.Value.Operation.InvokeAsync(new($"[EventProcessingHostedService.ConfigureDatabase/Starting] : retry count = {retryCount}"), cancellationToken);

                try
                {
                    await dbContext.Database.ExecuteSqlRawAsync($@"
EXEC sp_getapplock @Resource = '{typeof(EventProcessingHostedService).FullName}', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 15000;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}')
BEGIN
    EXEC('CREATE SCHEMA {schema}');
END

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{schema}].[Projection]') AND type in (N'U'))
BEGIN
    CREATE TABLE [{schema}].[Projection]
    (
	    [Name] [nvarchar](650) NOT NULL,
	    [SequenceNumber] [bigint] NOT NULL,
        CONSTRAINT [PK_Projection] PRIMARY KEY CLUSTERED 
        (
	        [Name] ASC
        )
        WITH 
        (
            PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
        ) ON [PRIMARY]
    ) ON [PRIMARY]
END

IF COL_LENGTH('[{schema}].[Projection]', 'LockedAt') IS NULL
BEGIN
    ALTER TABLE [{schema}].[Projection]
    ADD [LockedAt] DATETIMEOFFSET(7) NULL;
END

IF COL_LENGTH('[{schema}].[Projection]', 'DeferredUntil') IS NULL
BEGIN
    ALTER TABLE [{schema}].[Projection]
    ADD [DeferredUntil] DATETIMEOFFSET(7) NULL;
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = N'IX_Projection_SequenceNumber_Name' AND object_id = OBJECT_ID(N'[{schema}].[Projection]'))
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Projection_SequenceNumber_Name] 
    ON [{schema}].[Projection] 
    (
        [SequenceNumber] ASC, 
        [Name] ASC
    );
END

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{schema}].[ProjectionJournal]') AND type in (N'U'))
BEGIN
    DROP TABLE [{schema}].[ProjectionJournal];
END

EXEC sp_releaseapplock @Resource = '{typeof(EventProcessingHostedService).FullName}', @LockOwner = 'Session';
", cancellationToken);

                    await recallOptions.Value.Operation.InvokeAsync(new($"[EventProcessingHostedService.ConfigureDatabase/Completed] : retry count = {retryCount}"), cancellationToken);

                    retry = false;
                }
                catch (Exception ex)
                {
                    await recallOptions.Value.Operation.InvokeAsync(new($"[EventProcessingHostedService.ConfigureDatabase/Failed] : exception = {ex.AllMessages()}"), cancellationToken);

                    retryCount++;

                    if (retryCount > 3)
                    {
                        throw;
                    }
                }
            }
        }
        else
        {
            await recallOptions.Value.Operation.InvokeAsync(new("[EventProcessingHostedService.ConfigureDatabase/Disabled]"), cancellationToken);
        }

        await recallOptions.Value.Operation.InvokeAsync(new($"[EventProcessingHostedService.Projections] : count = {eventProcessorConfiguration.Projections.Count()}"), cancellationToken);

        foreach (var projectionConfiguration in eventProcessorConfiguration.Projections)
        {
            var connection = dbContext.Database.GetDbConnection();

            await using var command = connection.CreateCommand();

            command.CommandText = $@"
IF NOT EXISTS (SELECT NULL FROM [{schema}].[Projection] WHERE [Name] = @Name)
BEGIN
    INSERT INTO [{schema}].[Projection] 
    (
        [Name], 
        [SequenceNumber]
    ) 
    VALUES 
    (
        @Name, 
        0
    )
END
";

            command.Parameters.Add(new SqlParameter("@Name", projectionConfiguration.Name));

            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync(cancellationToken);
            }

            await command.ExecuteNonQueryAsync(cancellationToken);

            await recallOptions.Value.Operation.InvokeAsync(new($"[EventProcessingHostedService.Projections/Configured] : name = {projectionConfiguration.Name} / event type count = {projectionConfiguration.EventTypes.Count()}"), cancellationToken);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}