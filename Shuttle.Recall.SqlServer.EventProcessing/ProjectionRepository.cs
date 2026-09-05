using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using Shuttle.Recall.SqlServer.Storage;

namespace Shuttle.Recall.SqlServer.EventProcessing;

[SuppressMessage("Security", "EF1002:Risk of vulnerability to SQL injection", Justification = "Schema and table names are from trusted configuration sources")]
public class ProjectionRepository(ISqlServerStorageSchemaAccessor schemaAccessor, SqlServerEventProcessingDbContext dbContext)
    : IProjectionRepository
{
    public async Task<Projection> GetAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schemaAccessor);
        ArgumentNullException.ThrowIfNull(dbContext);

        var connection = dbContext.Database.GetDbConnection();

        await using var command = connection.CreateCommand();

        command.CommandText = $@"
IF NOT EXISTS (SELECT NULL FROM [{schemaAccessor.Schema}].[Projection] WHERE [Name] = @Name)
BEGIN
    INSERT INTO [{schemaAccessor.Schema}].[Projection] 
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

SELECT
    [Name],
    [SequenceNumber],
    [FailureCount]
FROM
    [{schemaAccessor.Schema}].[Projection]
WHERE
    [Name] = @Name
";

        command.Parameters.Add(new SqlParameter("@Name", name));

        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new ApplicationException(string.Format(Resources.ProjectionException));
        }

        return new(reader.GetString(0), reader.GetInt64(1), reader.GetInt32(2));
    }

    public async Task SaveAsync(Projection projection, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schemaAccessor);
        ArgumentNullException.ThrowIfNull(dbContext);
        ArgumentNullException.ThrowIfNull(projection);

        await dbContext.Database.ExecuteSqlRawAsync(@$"
UPDATE
    [{schemaAccessor.Schema}].[Projection]
SET
    [SequenceNumber] = @SequenceNumber,
    [LockedAt] = NULL,
    [DeferredUntil] = @DeferredUntil,
    [FailureCount] = @FailureCount
WHERE
    Name = @Name
",
            [
                new SqlParameter("@Name", projection.Name),
                new SqlParameter("@SequenceNumber", projection.SequenceNumber),
                new SqlParameter("@DeferredUntil", (object?)projection.DeferredUntil ?? DBNull.Value),
                new SqlParameter("@FailureCount", projection.FailureCount)
            ],
            cancellationToken);
    }
}