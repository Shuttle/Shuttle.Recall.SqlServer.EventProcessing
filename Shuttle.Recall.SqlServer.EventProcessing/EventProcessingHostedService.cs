using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore;

namespace Shuttle.Recall.SqlServer.EventProcessing;

[SuppressMessage("Security", "EF1002:Risk of vulnerability to SQL injection", Justification = "Schema and table names are from trusted configuration sources")]
public class EventProcessingHostedService(IOptions<PipelineOptions> pipelineOptions, IOptions<SqlServerEventProcessingOptions> sqlEventProcessingOptions, IDbContextFactory<SqlServerEventProcessingDbContext> dbContextFactory)
    : IHostedService
{
    private readonly PipelineOptions _pipelineOptions = Guard.AgainstNull(Guard.AgainstNull(pipelineOptions).Value);
    private readonly IDbContextFactory<SqlServerEventProcessingDbContext> _dbContextFactory = Guard.AgainstNull(dbContextFactory);
    private readonly SqlServerEventProcessingOptions _sqlServerEventProcessingOptions = Guard.AgainstNull(Guard.AgainstNull(sqlEventProcessingOptions).Value);
    private readonly Type _eventProcessorStartupPipelineType = typeof(EventProcessorStartupPipeline);

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _pipelineOptions.PipelineCreated += PipelineCreated;

        if (!_sqlServerEventProcessingOptions.ConfigureDatabase)
        {
            return;
        }

        var retry = true;
        var retryCount = 0;

        while (retry)
        {
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

                retry = false;
            }
            catch
            {
                retryCount++;

                if (retryCount > 3)
                {
                    throw;
                }
            }
        }
    }

    private Task PipelineCreated(PipelineEventArgs eventArgs, CancellationToken cancellationToken)
    {
        if (eventArgs.Pipeline.GetType() == _eventProcessorStartupPipelineType)
        {
            eventArgs.Pipeline.AddObserver<EventProcessingStartupObserver>();
        }

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _pipelineOptions.PipelineCreated -= PipelineCreated;

        await Task.CompletedTask;
    }
}