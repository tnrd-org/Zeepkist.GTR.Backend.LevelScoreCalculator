using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;
using Quartz;
using TNRD.Zeepkist.GTR.Database;
using TNRD.Zeepkist.GTR.Database.Models;

namespace Zeepkist.GTR.Backend.LevelScoreCalculator.Jobs;

public class GroupingJob : IJob
{
    public static readonly JobKey JobKey = new("Grouping");

    private readonly ILogger<GroupingJob> logger;
    private readonly GTRContext db;
    private readonly IMemoryCache memoryCache;

    public GroupingJob(GTRContext db, IMemoryCache memoryCache, ILogger<GroupingJob> logger)
    {
        this.db = db;
        this.memoryCache = memoryCache;
        this.logger = logger;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        List<IGrouping<string, Record>> groups = await db.Records
            .AsNoTracking()
            .OrderBy(x => x.Id)
            .GroupBy(x => x.Level)
            .ToListAsync(context.CancellationToken);

        foreach (IGrouping<string, Record> grouping in groups)
        {
            List<Record> records = grouping.OrderBy(x => x.DateCreated).ToList();
            if (records.Count == 0)
            {
                logger.LogInformation("Skipping {Level} because there are no records", grouping.Key);
                continue;
            }

            if (!memoryCache.TryGetValue(grouping.Key, out DateTime latestRecord))
            {
                latestRecord = DateTime.MinValue;
            }

            Record lastRecord = records.Last();
            if (lastRecord.DateCreated == latestRecord)
            {
                logger.LogInformation("Skipping {Level} because it has no new records", grouping.Key);
                continue;
            }

            memoryCache.Set(grouping.Key, lastRecord.DateCreated);

            await context.Scheduler.TriggerJob(CalculateJob.JobKey,
                new JobDataMap
                {
                    { CalculateJob.GROUP_KEY, grouping }
                },
                context.CancellationToken);
        }
    }
}
