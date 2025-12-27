using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using System.Data;

namespace Shuttle.Recall.SqlServer.EventProcessing;

public class ProjectionQuery(IOptions<SqlServerEventProcessingOptions> sqlServerEventProcessingOptions, IDbContextFactory<SqlServerEventProcessingDbContext> dbContextFactory)
    : IProjectionQuery
{
    private readonly SqlServerEventProcessingOptions _sqlServerEventProcessingOptions = Guard.AgainstNull(Guard.AgainstNull(sqlServerEventProcessingOptions).Value);
    private readonly IDbContextFactory<SqlServerEventProcessingDbContext> _dbContextFactory = Guard.AgainstNull(dbContextFactory);

    public async Task<IEnumerable<long>> GetIncompleteSequenceNumbersAsync(string name, CancellationToken cancellationToken = default)
    {
        Guard.AgainstEmpty(name);

        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var connection = dbContext.Database.GetDbConnection();

        await using var command = connection.CreateCommand();

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
}