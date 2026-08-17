using System.Data;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Options;
using Shuttle.Contract;
using Shuttle.Recall.SqlServer.Storage;

namespace Shuttle.Recall.SqlServer.EventProcessing;

public interface IImmediateProjectionEventRepository
{
    Task<bool> ContainsAsync(string projectionName, Guid eventId, CancellationToken cancellationToken = default);
    Task SaveAsync(string projectionName, Guid eventId, CancellationToken cancellationToken = default);
    Task RemoveAsync(string projectionName, Guid eventId, CancellationToken cancellationToken = default);
}

[SuppressMessage("Security", "EF1002:Risk of vulnerability to SQL injection", Justification = "Schema and table names are from trusted configuration sources")]
public class ImmediateProjectionEventRepository(IOptions<SqlServerStorageOptions> sqlServerStorageOptions, SqlServerStorageDbContext dbContext) : IImmediateProjectionEventRepository
{
    private readonly SqlServerStorageDbContext _dbContext = Guard.AgainstNull(dbContext);
    private readonly SqlServerStorageOptions _sqlServerStorageOptions = Guard.AgainstNull(Guard.AgainstNull(sqlServerStorageOptions).Value);

    public async Task<bool> ContainsAsync(string projectionName, Guid eventId, CancellationToken cancellationToken = default)
    {
        var connection = _dbContext.Database.GetDbConnection();

        await using var command = connection.CreateCommand();

        command.CommandText = $@"
IF EXISTS (SELECT NULL FROM [{_sqlServerStorageOptions.Schema}].[ImmediateProjectionEvent] WHERE [ProjectionName] = @ProjectionName AND [EventId] = @EventId)
    SELECT 1
ELSE
    SELECT 0
";

        command.Parameters.Add(new SqlParameter("@ProjectionName", Guard.AgainstEmpty(projectionName)));
        command.Parameters.Add(new SqlParameter("@EventId", eventId));

        var currentTransaction = _dbContext.Database.CurrentTransaction;

        if (currentTransaction != null)
        {
            command.Transaction = currentTransaction.GetDbTransaction();
        }

        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
        }

        return (int)(await command.ExecuteScalarAsync(cancellationToken) ?? 0) == 1;
    }

    public async Task SaveAsync(string projectionName, Guid eventId, CancellationToken cancellationToken = default)
    {
        await _dbContext.Database.ExecuteSqlRawAsync($@"
IF NOT EXISTS (SELECT NULL FROM [{_sqlServerStorageOptions.Schema}].[ImmediateProjectionEvent] WHERE [ProjectionName] = @ProjectionName AND [EventId] = @EventId)
BEGIN
    INSERT INTO [{_sqlServerStorageOptions.Schema}].[ImmediateProjectionEvent] ([ProjectionName], [EventId])
    VALUES (@ProjectionName, @EventId)
END
",
            [
                new SqlParameter("@ProjectionName", Guard.AgainstEmpty(projectionName)),
                new SqlParameter("@EventId", eventId)
            ],
            cancellationToken
        );
    }

    public async Task RemoveAsync(string projectionName, Guid eventId, CancellationToken cancellationToken = default)
    {
        await _dbContext.Database.ExecuteSqlRawAsync($@"
DELETE FROM [{_sqlServerStorageOptions.Schema}].[ImmediateProjectionEvent]
WHERE [ProjectionName] = @ProjectionName AND [EventId] = @EventId
",
            [
                new SqlParameter("@ProjectionName", Guard.AgainstEmpty(projectionName)),
                new SqlParameter("@EventId", eventId)
            ],
            cancellationToken
        );
    }
}
