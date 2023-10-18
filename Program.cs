using Quartz;
using Serilog;
using TNRD.Zeepkist.GTR.Backend.PersonalBestProcessor.Jobs;
using TNRD.Zeepkist.GTR.Database;

namespace TNRD.Zeepkist.GTR.Backend.PersonalBestProcessor;

internal class Program
{
    public static void Main(string[] args)
    {
        IHost host = Host.CreateDefaultBuilder(args)
            .UseSerilog((context, configuration) =>
            {
                configuration
                    .MinimumLevel.Debug()
                    .WriteTo.Console();
            })
            .ConfigureServices((context, services) =>
            {
                services.AddMemoryCache();
                services.AddNpgsql<GTRContext>(context.Configuration["Database:ConnectionString"]);
                services.AddQuartz(q =>
                {
                    q.AddJob<CalculateJob>(CalculateJob.JobKey, options => options.StoreDurably());
                    q.AddJob<UpdateJob>(UpdateJob.JobKey, options => options.StoreDurably());
                    q.AddJob<GroupingJob>(GroupingJob.JobKey)
                        .AddTrigger(options =>
                        {
                            options
                                .ForJob(GroupingJob.JobKey)
                                .WithIdentity(GroupingJob.JobKey.Name + "-Trigger")
                                .WithCronSchedule("0 0/15 * ? * * *");
                        });
                });

                services.AddQuartzHostedService(options =>
                {
                    options.AwaitApplicationStarted = true;
                    options.WaitForJobsToComplete = true;
                });
            })
            .Build();

        ILogger<Program> logger = host.Services.GetRequiredService<ILogger<Program>>();

        TaskScheduler.UnobservedTaskException += (sender, eventArgs) =>
        {
            logger.LogCritical(eventArgs.Exception, "Unobserved task exception");
        };

        AppDomain.CurrentDomain.UnhandledException += (sender, eventArgs) =>
        {
            logger.LogCritical(eventArgs.ExceptionObject as Exception, "Unhandled exception");
        };

        host.Run();
    }

    private static void CreateAndAddJob<TJob>(
        IServiceCollectionQuartzConfigurator q,
        string name,
        string cronSchedule,
        bool runAtStartup
    )
        where TJob : IJob
    {
        JobKey key = new($"{name}Job");
        q.AddJob<TJob>(opts => opts.WithIdentity(key));

        q.AddTrigger(opts =>
        {
            opts.ForJob(key)
                .WithIdentity($"{name}Job-Trigger")
                .WithCronSchedule(cronSchedule);
        });

        if (runAtStartup)
        {
            q.AddTrigger(opts =>
            {
                opts.ForJob(key)
                    .WithIdentity($"{name}Job-OnStartup")
                    .StartNow();
            });
        }
    }
}
