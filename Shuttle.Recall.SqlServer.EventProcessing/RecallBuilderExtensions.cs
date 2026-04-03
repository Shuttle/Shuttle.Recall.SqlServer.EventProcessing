using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
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
        public RecallBuilder UseSqlServerEventProcessing(Action<SqlServerEventProcessingOptions>? configureOptions)
        {
            var services = recallBuilder.Services;

            services.AddSingleton<IValidateOptions<SqlServerEventProcessingOptions>, SqlServerEventProcessingOptionsValidator>();
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
                var sqlServerEventProcessingOptions = serviceProvider.GetRequiredService<IOptions<SqlServerEventProcessingOptions>>().Value;
                var dbConnection = serviceProvider.GetKeyedService<DbConnection>(sqlServerEventProcessingOptions.DbConnectionServiceKey);

                if (dbConnection != null)
                {
                    var sqlConnectionStringBuilder = new SqlConnectionStringBuilder(sqlServerEventProcessingOptions.ConnectionString);

                    if (!dbConnection.Database.Equals(sqlConnectionStringBuilder.InitialCatalog, StringComparison.InvariantCultureIgnoreCase) ||
                        !dbConnection.DataSource.Equals(sqlConnectionStringBuilder.DataSource, StringComparison.InvariantCultureIgnoreCase))
                    {
                        throw new ApplicationException(Resources.DbConnectionException);
                    }

                    options.UseSqlServer(dbConnection, sqlServerOptions =>
                    {
                        sqlServerOptions.CommandTimeout((int)sqlServerEventProcessingOptions.CommandTimeout.TotalSeconds);
                    });
                }
                else
                {
                    options.UseSqlServer(sqlServerEventProcessingOptions.ConnectionString, sqlServerOptions =>
                    {
                        sqlServerOptions.CommandTimeout((int)sqlServerEventProcessingOptions.CommandTimeout.TotalSeconds);
                    });
                }
            });

            return recallBuilder;
        }
    }
}