namespace Shuttle.Recall.SqlServer.EventProcessing;

public class SqlServerEventProcessingOptions
{
    public const string SectionName = "Shuttle:Recall:SqlServer:EventProcessing";

    public int ProjectionPrefetchCount { get; set; } = 100;
    public int MaximumCacheSize { get; set; } = 1000;
    public TimeSpan CacheDuration { get; set; } = TimeSpan.FromMinutes(1);
    public TimeSpan ProjectionLockTimeout { get; set; } = TimeSpan.FromSeconds(30);
}