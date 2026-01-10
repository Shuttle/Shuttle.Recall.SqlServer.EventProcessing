using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Text;

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

    public async Task SaveAsync(Projection projection, CancellationToken cancellationToken = default)
    {
        Guard.AgainstNull(projection);

        await _dbContext.Database.ExecuteSqlRawAsync(@$"
UPDATE
    [{_sqlServerEventProcessingOptions.Schema}].[Projection]
SET
    SequenceNumber = @SequenceNumber
WHERE
    Name = @Name
",
            [
                new SqlParameter("@Name", projection.Name),
                new SqlParameter("@SequenceNumber", projection.SequenceNumber)
            ],
            cancellationToken);
    }

    public async Task CommitJournalSequenceNumbersAsync(string name, CancellationToken cancellationToken = default)
    {
        var sql = @$"
DECLARE @SequenceNumber BIGINT
DECLARE @IncompleteCount INT;

SELECT 
    @IncompleteCount = COUNT(*)
FROM 
    [{_sqlServerEventProcessingOptions.Schema}].[ProjectionJournal]
WHERE
    [Name] = @Name
AND
    DateCompleted IS NULL;

IF (@IncompleteCount > 0)
BEGIN
    DECLARE @ErrorMessage NVARCHAR(MAX);
    SET @ErrorMessage = 'Cannot commit projection sequence number: incomplete journal entries exist for projection ' + @Name;
    THROW 50001, @ErrorMessage, 1;
END;

SELECT 
    @SequenceNumber = MAX(SequenceNumber)
FROM 
    [{_sqlServerEventProcessingOptions.Schema}].[ProjectionJournal]
WHERE 
    [Name] = @Name
AND
    DateCompleted IS NOT NULL;

IF (@SequenceNumber IS NULL)
BEGIN
    RETURN;
END;

UPDATE 
    [{_sqlServerEventProcessingOptions.Schema}].[Projection] 
SET 
    SequenceNumber = @SequenceNumber
WHERE 
    [Name] = @Name
AND
    SequenceNumber < @SequenceNumber;

DELETE
FROM
    [{_sqlServerEventProcessingOptions.Schema}].[ProjectionJournal]
WHERE
    [Name] = @Name;
";
        await dbContext.Database.ExecuteSqlRawAsync(sql,
            [
                new SqlParameter("@Name", name)
            ],
            cancellationToken);
    }

    public async Task RegisterJournalSequenceNumbersAsync(string name, IEnumerable<long> sequenceNumbers, CancellationToken cancellationToken = default)
    {
        var sql = new StringBuilder($@"
DELETE
FROM
    [{_sqlServerEventProcessingOptions.Schema}].[ProjectionJournal]
WHERE
    [Name] = @Name;
");

        var numbers = sequenceNumbers.ToList();

        if (numbers.Any())
        {
            foreach (var chunk in numbers.Chunk(200))
            {
                sql.Append($@"
INSERT INTO 
    [{_sqlServerEventProcessingOptions.Schema}].[ProjectionJournal]
(
    [Name],
    [SequenceNumber]
)
VALUES
    {string.Join(",", chunk.Select(sequenceNumber => $"(@Name, {sequenceNumber})"))} 
;
");

                await _dbContext.Database.ExecuteSqlRawAsync(sql.ToString(),
                    [
                        new SqlParameter("@Name", name)
                    ],
                    cancellationToken);
            }
        }
    }

    public async Task CompleteAsync(ProjectionEvent projectionEvent, CancellationToken cancellationToken = default)
    {
        await _dbContext.Database.ExecuteSqlRawAsync(@$"
UPDATE
    [{_sqlServerEventProcessingOptions.Schema}].[ProjectionJournal]
SET
    DateCompleted = GETUTCDATE()
WHERE
    Name = @Name
AND 
    SequenceNumber = @SequenceNumber",
            [
                new SqlParameter("@Name", projectionEvent.Projection.Name),
                new SqlParameter("@SequenceNumber", projectionEvent.PrimitiveEvent.SequenceNumber!.Value)
            ],
            cancellationToken);
    }
}