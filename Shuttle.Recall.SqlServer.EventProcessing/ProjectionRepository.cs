using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace Shuttle.Recall.SqlServer.EventProcessing;

[SuppressMessage("Security", "EF1002:Risk of vulnerability to SQL injection", Justification = "Schema and table names are from trusted configuration sources")]
public class ProjectionRepository(IOptions<SqlServerEventProcessingOptions> sqlServerEventProcessingOptions, SqlServerEventProcessingDbContext dbContext)
    : IProjectionRepository
{
    private readonly SqlServerEventProcessingDbContext _dbContext = Guard.AgainstNull(dbContext);
    private readonly SqlServerEventProcessingOptions _sqlServerEventProcessingOptions = Guard.AgainstNull(Guard.AgainstNull(sqlServerEventProcessingOptions).Value);

    public async Task<Projection> GetAsync(string name, CancellationToken cancellationToken = default)
    {
        var connection = _dbContext.Database.GetDbConnection();

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

SELECT 
    [Name], 
    [SequenceNumber]
FROM 
    [{_sqlServerEventProcessingOptions.Schema}].[Projection] 
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

        return new(reader.GetString(0), reader.GetInt64(1));
    }

    public async Task CommitAsync(Projection projection, CancellationToken cancellationToken = default)
    {
        Guard.AgainstNull(projection);

        await _dbContext.Database.ExecuteSqlRawAsync(@$"
UPDATE
    [{_sqlServerEventProcessingOptions.Schema}].[Projection]
SET
    [SequenceNumber] = @SequenceNumber,
    [LockedAt] = NULL
WHERE
    Name = @Name
",
            [
                new SqlParameter("@Name", projection.Name),
                new SqlParameter("@SequenceNumber", projection.SequenceNumber)
            ],
            cancellationToken);
    }

    public async Task DeferAsync(Projection projection, DateTimeOffset deferredUntil, CancellationToken cancellationToken = default)
    {
        Guard.AgainstNull(projection);

        await _dbContext.Database.ExecuteSqlRawAsync(@$"
UPDATE
    [{_sqlServerEventProcessingOptions.Schema}].[Projection]
SET
    [LockedAt] = NULL,
    [DeferredUntil] = @DeferredUntil
WHERE
    Name = @Name
",
            [
                new SqlParameter("@Name", projection.Name),
                new SqlParameter("@DeferredUntil", deferredUntil)
            ],
            cancellationToken);
    }
}