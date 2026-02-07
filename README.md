# Shuttle.Recall.SqlServer.EventProcessing

A Sql Server implementation of the `Shuttle.Recall` event projection processing mechanism.

## Installation

```bash
dotnet add package Shuttle.Recall.SqlServer.EventProcessing
```

## Configuration

```c#
services.AddRecall(builder => 
{
    builder.UseSqlServerEventProcessing(options => 
    {
        options.ConnectionString = "connection-string";
    });
});
```

The default JSON settings structure is as follows:

```json
{
  "Shuttle": {
    "Recall": {
      "SqlServer": {
        "EventProcessing": {
          "ConnectionString": "connection-string",
          "Schema": "dbo",
          "CommandTimeout": "00:00:30",
          "ProjectionPrefetchCount": 100,
          "ConfigureDatabase": true,
          "MaximumCacheSize": 1000,
          "CacheDuration": "00:01:00",
          "ProjectionLockTimeout": "00:00:30"
        }
      }
    }
  }
}
```

## Database

In order to create the relevant database structures you can use the `Shuttle.Recall.SqlServer.EventProcessing.Database` console application and provide the `connection-string` and (optional) `schema` arguments.  Alternatively, you can let the library create the structures by setting the `ConfigureDatabase` option to `true` (which is the default).
