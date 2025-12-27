using Microsoft.EntityFrameworkCore;

namespace Shuttle.Recall.SqlServer.EventProcessing;

public class SqlServerEventProcessingDbContext(DbContextOptions<SqlServerEventProcessingDbContext> options) : DbContext(options);