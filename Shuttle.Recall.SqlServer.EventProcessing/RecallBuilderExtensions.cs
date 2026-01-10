using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Shuttle.Recall.SqlServer.EventProcessing;

public static class RecallBuilderExtensions
{
    extension(RecallBuilder recallBuilder)
    {
        public RecallBuilder UseSqlServerEventProcessing(Action<SqlServerEventProcessingBuilder>? builder = null)
        {
            var services = recallBuilder.Services;
            var sqlServerEventProcessingBuilder = new SqlServerEventProcessingBuilder(services);

            builder?.Invoke(sqlServerEventProcessingBuilder);

            services.TryAddSingleton<IValidateOptions<SqlServerEventProcessingOptions>, SqlServerEventProcessingOptionsValidator>();
            services.TryAddScoped<IProjectionQuery, ProjectionQuery>();
            services.TryAddScoped<IProjectionRepository, ProjectionRepository>();
            //.AddSingleton<PartitionedProjectionService>()
            //.AddSingleton<IProjectionService>(sp => sp.GetRequiredService<PartitionedProjectionService>())
            services.TryAddScoped<IProjectionEventService, SequentialProjectionEventService>();
            services.TryAddSingleton<ISequentialProjectionEventServiceContext, SequentialProjectionEventServiceContext>();

            services.AddOptions<SqlServerEventProcessingOptions>().Configure(options =>
            {
                options.ConnectionString = sqlServerEventProcessingBuilder.Options.ConnectionString;
                options.Schema = sqlServerEventProcessingBuilder.Options.Schema;
                options.CommandTimeout = sqlServerEventProcessingBuilder.Options.CommandTimeout;
                options.ConfigureDatabase = sqlServerEventProcessingBuilder.Options.ConfigureDatabase;
                options.ProjectionPrefetchCount = sqlServerEventProcessingBuilder.Options.ProjectionPrefetchCount;
                options.MaximumCacheSize = sqlServerEventProcessingBuilder.Options.MaximumCacheSize;
                options.CacheDuration = sqlServerEventProcessingBuilder.Options.CacheDuration;

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

            services.AddDbContextFactory<SqlServerEventProcessingDbContext>(dbContextFactoryBuilder =>
            {
                dbContextFactoryBuilder.UseSqlServer(sqlServerEventProcessingBuilder.Options.ConnectionString, sqlServerOptions =>
                {
                    sqlServerOptions.CommandTimeout(sqlServerEventProcessingBuilder.Options.CommandTimeout.Seconds);
                });
            });

            return recallBuilder;
        }
    }
}