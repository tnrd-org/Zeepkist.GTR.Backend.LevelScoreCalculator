using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;
using Quartz;
using TNRD.Zeepkist.GTR.Database;
using TNRD.Zeepkist.GTR.Database.Models;

namespace TNRD.Zeepkist.GTR.Backend.LevelScoreCalculator.Jobs;

public class FilterLevelsJob : IJob
{
    public static readonly JobKey JobKey = new("FilterLevels");

    private readonly ILogger<FilterLevelsJob> logger;
    private readonly GTRContext db;
    private readonly IMemoryCache memoryCache;

    public FilterLevelsJob(GTRContext db, IMemoryCache memoryCache, ILogger<FilterLevelsJob> logger)
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

        for (int i = 0; i < groups.Count; i++)
        {
            IGrouping<string, Record> grouping = groups[i];
            List<Record> records = grouping
                .OrderByDescending(x => x.DateCreated)
                .Take(1)
                .ToList();

            if (records.Count == 0)
            {
                logger.LogInformation("Skipping {Level} because there are no records", grouping.Key);
                continue;
            }

            if (!memoryCache.TryGetValue(grouping.Key, out DateTime latestRecord))
            {
                latestRecord = DateTime.MinValue;
            }

            Record lastRecord = records.First();
            if (lastRecord.DateCreated == latestRecord)
            {
                logger.LogInformation("Skipping {Level} because it has no new records", grouping.Key);
                continue;
            }

            memoryCache.Set(grouping.Key, lastRecord.DateCreated);

            await context.Scheduler.TriggerJob(CalculateLevelScoreJob.JobKey,
                new JobDataMap
                {
                    { CalculateLevelScoreJob.GROUP_KEY, grouping }
                },
                context.CancellationToken);
        }
    }
}
