using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System.Data.Common;

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
            services.TryAddScoped<IProjectionEventService, SequentialProjectionEventService>();
            services.TryAddSingleton<ISequentialProjectionEventServiceContext, SequentialProjectionEventServiceContext>();

            services.AddOptions<SqlServerEventProcessingOptions>().Configure(options =>
            {
                options.ConnectionString = sqlServerEventProcessingBuilder.Options.ConnectionString;
                options.Schema = sqlServerEventProcessingBuilder.Options.Schema;
                options.CommandTimeout = sqlServerEventProcessingBuilder.Options.CommandTimeout;
                options.ConfigureDatabase = sqlServerEventProcessingBuilder.Options.ConfigureDatabase;
                options.ProjectionPrefetchCount = sqlServerEventProcessingBuilder.Options.ProjectionPrefetchCount;
                options.ProjectionLockTimeout = sqlServerEventProcessingBuilder.Options.ProjectionLockTimeout;
                options.MaximumCacheSize = sqlServerEventProcessingBuilder.Options.MaximumCacheSize;
                options.CacheDuration = sqlServerEventProcessingBuilder.Options.CacheDuration;
                options.DbConnectionServiceKey = sqlServerEventProcessingBuilder.Options.DbConnectionServiceKey;

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

            services.AddDbContext<SqlServerEventProcessingDbContext>((sp, options) =>
            {
                var dbConnection = sp.GetKeyedService<DbConnection>(sqlServerEventProcessingBuilder.Options.DbConnectionServiceKey);

                if (dbConnection != null)
                {
                    var sqlConnectionStringBuilder = new SqlConnectionStringBuilder(sqlServerEventProcessingBuilder.Options.ConnectionString);

                    if (!dbConnection.Database.Equals(sqlConnectionStringBuilder.InitialCatalog, StringComparison.InvariantCultureIgnoreCase) ||
                        !dbConnection.DataSource.Equals(sqlConnectionStringBuilder.DataSource, StringComparison.InvariantCultureIgnoreCase))
                    {
                        throw new ApplicationException(Resources.DbConnectionException);
                    }

                    options.UseSqlServer(dbConnection, Configure);
                }
                else
                {
                    options.UseSqlServer(sqlServerEventProcessingBuilder.Options.ConnectionString, Configure);
                }
            });

            return recallBuilder;

            void Configure(SqlServerDbContextOptionsBuilder sqlServerOptions)
            {
                sqlServerOptions.CommandTimeout(sqlServerEventProcessingBuilder.Options.CommandTimeout.Seconds);
            }
        }
    }
}