namespace Shuttle.Recall.SqlServer.EventProcessing;

public class SqlServerEventProcessingOptions
{
    public const string SectionName = "Shuttle:EventStore:SqlServer:EventProcessing";

    public string ConnectionString { get; set; } = string.Empty;
    public string Schema { get; set; } = "dbo";
    public int CommandTimeout { get; set; } = 30;
    public int ProjectionBatchSize { get; set; } = 1000;
    public bool ConfigureDatabase { get; set; } = true;
    public bool RegisterDatabaseContextObserver { get; set; } = true;
}