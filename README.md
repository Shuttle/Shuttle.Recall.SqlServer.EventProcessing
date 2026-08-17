# Shuttle.Recall.SqlServer.EventProcessing

A Sql Server implementation of the `Shuttle.Recall` event projection processing mechanism.

## Installation

```bash
dotnet add package Shuttle.Recall.SqlServer.EventProcessing
```

## Configuration

This package processes projections against events already stored via `Shuttle.Recall.SqlServer.Storage` — it reuses that package's connection, schema, and database-creation settings, so `UseSqlServerEventStorage` must be configured alongside `UseSqlServerEventProcessing`:

```c#
services
    .AddRecall()
    .UseSqlServerEventStorage(options =>
    {
        options.ConnectionString = "connection-string";
        options.Schema = "dbo";
    })
    .UseSqlServerEventProcessing(options =>
    {
        options.ProjectionPrefetchCount = 100;
    });
```

`SqlServerEventProcessingOptions` has the following properties:

| Property | Default | Description |
|----------|---------|-------------|
| `ProjectionPrefetchCount` | `100` | Number of projection events fetched per round-trip |
| `MaximumCacheSize` | `1000` | Maximum number of entries kept in the event-type cache. Values above `100000` are clamped to `100000` |
| `CacheDuration` | `00:01:00` | How long cached entries are retained. Values above `01:00:00` are clamped to `01:00:00` |
| `ProjectionLockTimeout` | `00:00:30` | Timeout applied when acquiring a projection's processing lock |

`ConnectionString`, `Schema`, and `ConfigureDatabase` are **not** properties of `SqlServerEventProcessingOptions` — they come from `SqlServerStorageOptions` (see [Shuttle.Recall.SqlServer.Storage](https://www.nuget.org/packages/Shuttle.Recall.SqlServer.Storage)) and are shared between both packages.

`SqlServerEventProcessingOptions.SectionName` is `"Shuttle:Recall:SqlServer:EventProcessing"`, so the equivalent `appsettings.json` shape (for the properties that belong to this package) is:

```json
{
  "Shuttle": {
    "Recall": {
      "SqlServer": {
        "EventProcessing": {
          "ProjectionPrefetchCount": 100,
          "MaximumCacheSize": 1000,
          "CacheDuration": "00:01:00",
          "ProjectionLockTimeout": "00:00:30"
        },
        "Storage": {
          "ConnectionString": "connection-string",
          "Schema": "dbo",
          "ConfigureDatabase": true
        }
      }
    }
  }
}
```

> Neither package binds these sections automatically — use `services.Configure<SqlServerEventProcessingOptions>(configuration.GetSection(SqlServerEventProcessingOptions.SectionName))` (and the equivalent for `SqlServerStorageOptions`) if you want to configure from `appsettings.json`.

## Database

In order to create the relevant database structures you can use the `Shuttle.Recall.SqlServer.EventProcessing.Database` console application and provide the `connection-string` and (optional) `schema` arguments:

```bash
Shuttle.Recall.SqlServer.EventProcessing.Database --connection-string "connection-string" --schema "dbo"
```

Alternatively, you can let the library create the structures by setting `SqlServerStorageOptions.ConfigureDatabase` to `true` (which is the default).

## Immediate Consistency

When a projection is configured for immediate consistency (see the `Shuttle.Recall` documentation's `EventProcessingOptions.ImmediateConsistency` options), this package tracks each event that a projection has already handled immediately in an `ImmediateProjectionEvent` table, via `IImmediateProjectionEventRepository`. This allows the eventual event processor to skip re-invoking that projection's handler for an event it has already handled immediately, while still advancing the projection's checkpoint over it.

This package provides `SequentialProjectionEventService`, a Sql Server-backed implementation of `Shuttle.Recall`'s `IProjectionEventService`, which drives retrieval, acknowledgement, and deferral of projection events against the `PrimitiveEvent`/`Projection`/`ImmediateProjectionEvent` tables.
