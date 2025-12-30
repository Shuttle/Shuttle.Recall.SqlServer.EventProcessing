using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Recall.SqlServer.Storage;

namespace Shuttle.Recall.SqlServer.EventProcessing;

public static class ServiceCollectionExtensions
{
    extension(RecallBuilder recallBuilder)
    {
        public RecallBuilder UseSqlServerEventProcessing(Action<SqlServerEventProcessingBuilder>? builder = null)
        {
            var services = recallBuilder.Services;
            var sqlServerEventProcessingBuilder = new SqlServerEventProcessingBuilder(services);

            builder?.Invoke(sqlServerEventProcessingBuilder);

            services
                .AddSingleton<IValidateOptions<SqlServerEventProcessingOptions>, SqlServerEventProcessingOptionsValidator>()
                .AddSingleton<IProjectionQuery, ProjectionQuery>()
                .AddSingleton<IProjectionRepository, ProjectionRepository>()
                .AddSingleton<ProjectionService>()
                .AddSingleton<IProjectionService>(sp => sp.GetRequiredService<ProjectionService>());

            services.AddOptions<SqlServerEventProcessingOptions>().Configure(options =>
            {
                options.ConnectionString = sqlServerEventProcessingBuilder.Options.ConnectionString;
                options.Schema = sqlServerEventProcessingBuilder.Options.Schema;
                options.CommandTimeout = sqlServerEventProcessingBuilder.Options.CommandTimeout;
                options.ConfigureDatabase = sqlServerEventProcessingBuilder.Options.ConfigureDatabase;
                options.RegisterDatabaseContextObserver = sqlServerEventProcessingBuilder.Options.RegisterDatabaseContextObserver;
                options.ProjectionBatchSize = sqlServerEventProcessingBuilder.Options.ProjectionBatchSize;
            });

            recallBuilder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, EventProcessingHostedService>());

            services.AddDbContextFactory<SqlServerEventProcessingDbContext>(dbContextFactoryBuilder =>
            {
                dbContextFactoryBuilder.UseSqlServer(sqlServerEventProcessingBuilder.Options.ConnectionString, sqlServerOptions =>
                {
                    sqlServerOptions.CommandTimeout(sqlServerEventProcessingBuilder.Options.CommandTimeout);
                });
            });

            return recallBuilder;
        }
    }
}