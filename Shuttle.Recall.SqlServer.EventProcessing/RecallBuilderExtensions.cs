using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Shuttle.Recall.SqlServer.Storage;

namespace Shuttle.Recall.SqlServer.EventProcessing;

public static class RecallBuilderExtensions
{
    extension(RecallBuilder recallBuilder)
    {
        public RecallBuilder UseSqlServerEventProcessing(Action<SqlServerEventProcessingOptions>? configureOptions = null)
        {
            var services = recallBuilder.Services;

            services.AddScoped<IProjectionQuery, ProjectionQuery>();
            services.AddScoped<IProjectionRepository, ProjectionRepository>();
            services.AddScoped<IProjectionEventService, SequentialProjectionEventService>();
            services.AddSingleton<ISequentialProjectionEventServiceContext, SequentialProjectionEventServiceContext>();

            services.AddOptions<SqlServerEventProcessingOptions>().Configure(options =>
            {
                configureOptions?.Invoke(options);

                if (options.MaximumCacheSize > 100_000)
                {
                    options.MaximumCacheSize = 100_000;
                }

                if (options.CacheDuration > TimeSpan.FromHours(1))
                {
                    options.CacheDuration = TimeSpan.FromHours(1);
                }
            });

            recallBuilder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, EventProcessingHostedService>());

            services.AddDbContext<SqlServerEventProcessingDbContext>((serviceProvider, options) =>
            {
                var sqlServerStorageOptions = serviceProvider.GetRequiredService<IOptions<SqlServerStorageOptions>>().Value;
                var dbConnection = serviceProvider.GetRequiredKeyedService<DbConnection>(sqlServerStorageOptions.DbConnectionServiceKey);

                options.UseSqlServer(dbConnection, sqlServerOptions =>
                {
                    sqlServerOptions.CommandTimeout((int)sqlServerStorageOptions.CommandTimeout.TotalSeconds);
                });
            });

            return recallBuilder;
        }
    }
}