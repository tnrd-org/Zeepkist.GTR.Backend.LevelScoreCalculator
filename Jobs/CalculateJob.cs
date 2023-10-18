using Quartz;
using TNRD.Zeepkist.GTR.Database.Models;

namespace Zeepkist.GTR.Backend.LevelScoreCalculator.Jobs;

public class CalculateJob : IJob
{
    public const string GROUP_KEY = "Group";

    public static readonly JobKey JobKey = new("Calculate");

    private readonly ILogger<CalculateJob> logger;

    public CalculateJob(ILogger<CalculateJob> logger)
    {
        this.logger = logger;
    }

    public Task Execute(IJobExecutionContext context)
    {
        IGrouping<string, Record> grouping = (IGrouping<string, Record>)context.MergedJobDataMap.Get(GROUP_KEY);
        logger.LogInformation("Starting job for level {Level} with {Amount} records", grouping.Key, grouping.Count());

        List<Record> records = grouping.OrderBy(x => x.DateCreated).ToList();
        if (records.Count == 0)
            return Task.CompletedTask;

        float fastestTime = float.MaxValue;
        int timesBeaten = 0;
        HashSet<int> userIds = new();

        foreach (Record record in records)
        {
            userIds.Add(record.User);

            if (record.Time < fastestTime)
            {
                fastestTime = record.Time;
                timesBeaten++;
            }
        }

        context.Scheduler.TriggerJob(UpdateJob.JobKey,
            new JobDataMap()
            {
                { UpdateJob.LEVEL_KEY, grouping.Key },
                { UpdateJob.POINTS_KEY, timesBeaten + userIds.Count }
            },
            context.CancellationToken);

        return Task.CompletedTask;
    }
}
