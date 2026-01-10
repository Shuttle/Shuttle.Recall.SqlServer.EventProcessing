using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using System.Data;

namespace Shuttle.Recall.SqlServer.EventProcessing;

public class ProjectionQuery(IOptions<RecallOptions> recallOptions, IOptions<SqlServerEventProcessingOptions> sqlServerEventProcessingOptions, SqlServerEventProcessingDbContext dbContext)
    : IProjectionQuery
{
    private static readonly string ResourceName = typeof(ProjectionQuery).FullName ?? nameof(ProjectionQuery);

    private readonly RecallOptions _recallOptions = Guard.AgainstNull(Guard.AgainstNull(recallOptions).Value);
    private readonly SqlServerEventProcessingOptions _sqlServerEventProcessingOptions = Guard.AgainstNull(Guard.AgainstNull(sqlServerEventProcessingOptions).Value);
    private readonly SqlServerEventProcessingDbContext _dbContext = Guard.AgainstNull(dbContext);

    public async Task<IEnumerable<long>> GetIncompleteSequenceNumbersAsync(string name, CancellationToken cancellationToken = default)
    {
        Guard.AgainstEmpty(name);

        var connection = _dbContext.Database.GetDbConnection();

        await using var command = connection.CreateCommand();

        command.Transaction = _dbContext.Database.CurrentTransaction?.GetDbTransaction();
        
        command.CommandText = $@"
SELECT
    [SequenceNumber]
FROM
    [{_sqlServerEventProcessingOptions.Schema}].[ProjectionJournal]
WHERE
    [Name] = @Name
AND
    [DateCompleted] IS NULL
";

        command.Parameters.Add(new SqlParameter("@Name", name));

        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
        }
        
        var result = new List<long>();

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(reader.GetInt64(0));
        }

        return result;
    }

    public async ValueTask<Projection?> GetAsync(CancellationToken cancellationToken = default)
    {
        await _recallOptions.Operation.InvokeAsync(new("[ProjectionQuery.Get/Starting]"), cancellationToken);

        var connection = _dbContext.Database.GetDbConnection();

        await using var command = connection.CreateCommand();

        command.Transaction = _dbContext.Database.CurrentTransaction?.GetDbTransaction();
        
        command.CommandText = $@"
EXEC sp_getapplock @Resource = '{ResourceName}', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 15000;

DECLARE @SequenceNumber BIGINT;
DECLARE @Name VARCHAR(650);

;WITH cte AS
(
    SELECT TOP (1)
        p.[SequenceNumber],
        p.[Name]
    FROM 
        [{_sqlServerEventProcessingOptions.Schema}].[Projection] p
    WITH (UPDLOCK, READPAST, ROWLOCK)
    ORDER BY
        p.[SequenceNumber],
        p.[Name]
)
UPDATE 
    cte
SET 
    SequenceNumber = SequenceNumber
OUTPUT
    inserted.[Name],
    inserted.[SequenceNumber];

EXEC sp_releaseapplock @Resource = '{ResourceName}', @LockOwner = 'Session';
";

        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            await _recallOptions.Operation.InvokeAsync(new($"[ProjectionQuery.Get/Completed] : projection = <null>"), cancellationToken);

            return null;
        }

        var result = new Projection(reader.GetString(0), reader.GetInt64(1));

        await _recallOptions.Operation.InvokeAsync(new($"[ProjectionQuery.Get/Completed] : projection name = '{result.Name}' / sequence number = {result.SequenceNumber}"), cancellationToken);

        return result;
    }
}