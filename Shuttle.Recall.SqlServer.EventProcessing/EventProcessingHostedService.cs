using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Reflection;
using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace Shuttle.Recall.SqlServer.EventProcessing;

[SuppressMessage("Security", "EF1002:Risk of vulnerability to SQL injection", Justification = "Schema and table names are from trusted configuration sources")]
public class EventProcessingHostedService(IOptions<RecallOptions> recallOptions, IOptions<SqlServerEventProcessingOptions> sqlEventProcessingOptions, IDbContextFactory<SqlServerEventProcessingDbContext> dbContextFactory, IEventProcessorConfiguration eventProcessorConfiguration)
    : IHostedService
{
    private readonly RecallOptions _recallOptions = Guard.AgainstNull(Guard.AgainstNull(recallOptions).Value);
    private readonly IDbContextFactory<SqlServerEventProcessingDbContext> _dbContextFactory = Guard.AgainstNull(dbContextFactory);
    private readonly SqlServerEventProcessingOptions _sqlServerEventProcessingOptions = Guard.AgainstNull(Guard.AgainstNull(sqlEventProcessingOptions).Value);
    private readonly IEventProcessorConfiguration _eventProcessorConfiguration = Guard.AgainstNull(eventProcessorConfiguration);

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await using (var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken))
        {
            await _recallOptions.Operation.InvokeAsync(new($"[EventProcessingHostedService.Projections] : count = {_eventProcessorConfiguration.Projections.Count()}"), cancellationToken);

            foreach (var projectionConfiguration in _eventProcessorConfiguration.Projections)
            {
                var connection = dbContext.Database.GetDbConnection();

                await using var command = connection.CreateCommand();

                command.CommandText = $@"
IF NOT EXISTS (SELECT NULL FROM [{_sqlServerEventProcessingOptions.Schema}].[Projection] WHERE [Name] = @Name)
BEGIN
    INSERT INTO [{_sqlServerEventProcessingOptions.Schema}].[Projection] 
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

                await _recallOptions.Operation.InvokeAsync(new($"[EventProcessingHostedService.Projections/Configured] : name = {projectionConfiguration.Name} / event type count = {projectionConfiguration.EventTypes.Count()}"), cancellationToken);
            }
        }

        if (!_sqlServerEventProcessingOptions.ConfigureDatabase)
        {
            await _recallOptions.Operation.InvokeAsync(new("[EventProcessingHostedService.ConfigureDatabase/Disabled]"), cancellationToken);
            return;
        }

        var retry = true;
        var retryCount = 0;

        while (retry)
        {
            await _recallOptions.Operation.InvokeAsync(new($"[EventProcessingHostedService.ConfigureDatabase/Starting] : retry count = {retryCount}"), cancellationToken);

            try
            {
                await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

                await dbContext.Database.ExecuteSqlRawAsync($@"
EXEC sp_getapplock @Resource = '{typeof(EventProcessingHostedService).FullName}', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 15000;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{_sqlServerEventProcessingOptions.Schema}')
BEGIN
    EXEC('CREATE SCHEMA {_sqlServerEventProcessingOptions.Schema}');
END

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{_sqlServerEventProcessingOptions.Schema}].[Projection]') AND type in (N'U'))
BEGIN
    CREATE TABLE [{_sqlServerEventProcessingOptions.Schema}].[Projection]
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

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = N'IX_Projection_SequenceNumber_Name' AND object_id = OBJECT_ID(N'[{_sqlServerEventProcessingOptions.Schema}].[Projection]'))
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Projection_SequenceNumber_Name] 
    ON [{_sqlServerEventProcessingOptions.Schema}].[Projection] 
    (
        [SequenceNumber] ASC, 
        [Name] ASC
    );
END

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{_sqlServerEventProcessingOptions.Schema}].[ProjectionJournal]') AND type in (N'U'))
BEGIN
    CREATE TABLE [{_sqlServerEventProcessingOptions.Schema}].[ProjectionJournal]
    (
	    [Name] [nvarchar](650) NOT NULL,
	    [SequenceNumber] [bigint] NOT NULL,
	    [DateCompleted] [datetime2](7) NULL,
        CONSTRAINT [PK_ProjectionJournal] PRIMARY KEY CLUSTERED 
        (
	        [Name] ASC,
	        [SequenceNumber] ASC
        )
        WITH 
        (
            PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
        ) ON [PRIMARY]
    ) ON [PRIMARY]
END

EXEC sp_releaseapplock @Resource = '{typeof(EventProcessingHostedService).FullName}', @LockOwner = 'Session';
", cancellationToken: cancellationToken);

                await _recallOptions.Operation.InvokeAsync(new($"[EventProcessingHostedService.ConfigureDatabase/Completed] : retry count = {retryCount}"), cancellationToken);

                retry = false;
            }
            catch(Exception ex)
            {
                await _recallOptions.Operation.InvokeAsync(new($"[EventProcessingHostedService.ConfigureDatabase/Failed] : exception = {ex.AllMessages()}"), cancellationToken);

                retryCount++;

                if (retryCount > 3)
                {
                    throw;
                }
            }
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}