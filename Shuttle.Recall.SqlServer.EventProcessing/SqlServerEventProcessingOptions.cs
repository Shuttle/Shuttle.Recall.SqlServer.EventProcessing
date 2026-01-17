namespace Shuttle.Recall.SqlServer.EventProcessing;

public class SqlServerEventProcessingOptions
{
    public const string SectionName = "Shuttle:Recall:SqlServer:EventProcessing";

    public string ConnectionString { get; set; } = string.Empty;
    public string Schema { get; set; } = "dbo";
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public int ProjectionPrefetchCount { get; set; } = 100;
    public bool ConfigureDatabase { get; set; } = true;
    public int MaximumCacheSize { get; set; } = 1000;
    public TimeSpan CacheDuration { get; set; } = TimeSpan.FromMinutes(1);
    public TimeSpan ProjectionLockTimeout { get; set; } = TimeSpan.FromSeconds(30);
}