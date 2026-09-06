using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Options;
using System.Data;
using Shuttle.Recall.SqlServer.Storage;

namespace Shuttle.Recall.SqlServer.EventProcessing;

public class ProjectionQuery(IOptions<RecallOptions> recallOptions, ISqlServerStorageSchemaAccessor schemaAccessor, IOptions<SqlServerEventProcessingOptions> sqlServerEventProcessingOptions, SqlServerEventProcessingDbContext dbContext)
    : IProjectionQuery, IProjectionEligibilityQuery
{
    private static readonly string ResourceName = typeof(ProjectionQuery).FullName ?? nameof(ProjectionQuery);

    public async ValueTask<Query.Projection> SearchAsync(Query.Projection.Specification specification, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(specification);

        var connection = dbContext.Database.GetDbConnection();

        await using var command = connection.CreateCommand();

        command.Transaction = dbContext.Database.CurrentTransaction?.GetDbTransaction();

        command.CommandText = $@"
SELECT TOP (1)
    [Name],
    [SequenceNumber],
    [FailureCount],
    [DeferredUntil]
FROM
    [{schemaAccessor.Schema}].[Projection]
WHERE
    [Name] LIKE '%' + @NameMatch + '%'
AND
(
    @FailureCountStart IS NULL
    OR
    [FailureCount] >= @FailureCountStart
)
AND
(
    @SequenceNumberStart IS NULL
    OR
    [SequenceNumber] >= @SequenceNumberStart
)
AND
(
    @Deferred IS NULL
    OR
    (@Deferred = 1 AND [DeferredUntil] IS NOT NULL)
    OR
    (@Deferred = 0 AND [DeferredUntil] IS NULL)
)
ORDER BY
    [SequenceNumber],
    [Name]
";

        command.Parameters.Add(new SqlParameter("@NameMatch", specification.NameMatch));
        command.Parameters.Add(new SqlParameter("@FailureCountStart", (object?)specification.FailureCountStart ?? DBNull.Value));
        command.Parameters.Add(new SqlParameter("@SequenceNumberStart", (object?)specification.SequenceNumberStart ?? DBNull.Value));
        command.Parameters.Add(new SqlParameter("@Deferred", (object?)specification.Deferred ?? DBNull.Value));

        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new ApplicationException(Resources.ProjectionSearchException);
        }

        return new()
        {
            Name = reader.GetString(0),
            SequenceNumber = reader.GetInt64(1),
            FailureCount = reader.GetInt32(2),
            DeferredUntil = reader.IsDBNull(3) ? null : reader.GetFieldValue<DateTimeOffset>(3)
        };
    }

    public async ValueTask<Query.Projection?> GetPendingAsync(CancellationToken cancellationToken = default)
    {
        await recallOptions.Value.Operation.InvokeAsync(new("[ProjectionQuery.Get/Starting]"), cancellationToken);

        var connection = dbContext.Database.GetDbConnection();

        await using var command = connection.CreateCommand();

        command.Transaction = dbContext.Database.CurrentTransaction?.GetDbTransaction();
        
        command.CommandText = $@"
EXEC sp_getapplock @Resource = '{ResourceName}', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 15000;

DECLARE @SequenceNumber BIGINT;
DECLARE @Name VARCHAR(650);
DECLARE @Now DATETIMEOFFSET = SYSDATETIMEOFFSET();

;WITH cte AS
(
    SELECT TOP (1)
        p.[SequenceNumber],
        p.[Name],
        p.[LockedAt],
        p.[DeferredUntil],
        p.[FailureCount]
    FROM 
        [{schemaAccessor.Schema}].[Projection] p WITH (UPDLOCK, READPAST, ROWLOCK)
    WHERE
        {(recallOptions.Value.EventProcessing.IncludedProjections.Count > 0
            ? $"p.[Name] IN ({string.Join(',', recallOptions.Value.EventProcessing.IncludedProjections.Select(item => $"'{item}'"))}) AND"
            : string.Empty
        )}
        {(recallOptions.Value.EventProcessing.ExcludedProjections.Count > 0
            ? $"p.[Name] NOT IN ({string.Join(',', recallOptions.Value.EventProcessing.ExcludedProjections.Select(item => $"'{item}'"))}) AND"
            : string.Empty
        )}
        (
            p.[LockedAt] IS NULL
            OR
            p.[LockedAt] < @LockedAtTimeout
        )
        AND
        (
            p.[DeferredUntil] IS NULL
            OR
            p.[DeferredUntil] < @Now
        )
    ORDER BY
        p.[SequenceNumber],
        p.[Name]
)
UPDATE
    cte
SET
    [LockedAt] = @Now,
    [DeferredUntil] = NULL
OUTPUT
    inserted.[Name],
    inserted.[SequenceNumber],
    inserted.[FailureCount];

EXEC sp_releaseapplock @Resource = '{ResourceName}', @LockOwner = 'Session';
";

        command.Parameters.Add(new SqlParameter("@LockedAtTimeout", DateTimeOffset.UtcNow.Subtract(sqlServerEventProcessingOptions.Value.ProjectionLockTimeout)));

        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            await recallOptions.Value.Operation.InvokeAsync(new("[ProjectionQuery.Get/Completed] : projection = <null>"), cancellationToken);

            return null;
        }

        var result = new Query.Projection{
            Name=reader.GetString(0),
            SequenceNumber=reader.GetInt64(1),
            FailureCount=reader.GetInt32(2)
        };

        await recallOptions.Value.Operation.InvokeAsync(new($"[ProjectionQuery.Get/Completed] : projection name = '{result.Name}' / sequence number = {result.SequenceNumber}"), cancellationToken);

        return result;
    }

    public async ValueTask<bool> HasEligibleProjectionAsync(CancellationToken cancellationToken = default)
    {
        var connection = dbContext.Database.GetDbConnection();

        await using var command = connection.CreateCommand();

        command.Transaction = dbContext.Database.CurrentTransaction?.GetDbTransaction();

        command.CommandText = $@"
IF EXISTS
(
    SELECT
        NULL
    FROM
        [{schemaAccessor.Schema}].[Projection] p
    WHERE
        {(recallOptions.Value.EventProcessing.IncludedProjections.Count > 0
            ? $"p.[Name] IN ({string.Join(',', recallOptions.Value.EventProcessing.IncludedProjections.Select(item => $"'{item}'"))}) AND"
            : string.Empty
        )}
        {(recallOptions.Value.EventProcessing.ExcludedProjections.Count > 0
            ? $"p.[Name] NOT IN ({string.Join(',', recallOptions.Value.EventProcessing.ExcludedProjections.Select(item => $"'{item}'"))}) AND"
            : string.Empty
        )}
        (
            p.[LockedAt] IS NULL
            OR
            p.[LockedAt] < @LockedAtTimeout
        )
        AND
        (
            p.[DeferredUntil] IS NULL
            OR
            p.[DeferredUntil] < @Now
        )
)
    SELECT 1
ELSE
    SELECT 0
";

        command.Parameters.Add(new SqlParameter("@LockedAtTimeout", DateTimeOffset.UtcNow.Subtract(sqlServerEventProcessingOptions.Value.ProjectionLockTimeout)));
        command.Parameters.Add(new SqlParameter("@Now", DateTimeOffset.UtcNow));

        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
        }

        return (int)(await command.ExecuteScalarAsync(cancellationToken) ?? 0) == 1;
    }

    public async ValueTask<bool> HasPendingProjectionsAsync(long sequenceNumber, CancellationToken cancellationToken = default)
    {
        var connection = dbContext.Database.GetDbConnection();

        await using var command = connection.CreateCommand();

        command.CommandText = @$"
IF EXISTS
(
    SELECT
        NULL
    FROM
        [{schemaAccessor.Schema}].Projection
    WHERE
        SequenceNumber < @SequenceNumber
)
    SELECT 1
ELSE
    SELECT 0
";

        command.Parameters.Add(new SqlParameter("@SequenceNumber", sequenceNumber));

        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
        }

        return (int)(await command.ExecuteScalarAsync(cancellationToken) ?? 1) == 1;
    }
}