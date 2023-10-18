using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Quartz;
using TNRD.Zeepkist.GTR.Database;
using TNRD.Zeepkist.GTR.Database.Models;

namespace TNRD.Zeepkist.GTR.Backend.LevelScoreCalculator.Jobs;

public class UpdateJob : IJob
{
    public const string LEVEL_KEY = "Level";
    public const string POINTS_KEY = "Points";

    public static readonly JobKey JobKey = new("Update");

    private readonly ILogger<UpdateJob> logger;
    private readonly GTRContext db;

    public UpdateJob(ILogger<UpdateJob> logger, GTRContext db)
    {
        this.logger = logger;
        this.db = db;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        // This will change to a string
        string level = context.MergedJobDataMap.GetString(LEVEL_KEY)!;
        int points = context.MergedJobDataMap.GetIntValue(POINTS_KEY);

        logger.LogInformation("Updating level {Level} with {Points} points", level, points);

        LevelPoints? levelPoints = await db.LevelPoints
            .AsNoTracking()
            .Where(x => x.Level == level)
            .FirstOrDefaultAsync(context.CancellationToken);

        if (levelPoints != null && levelPoints.Points == points)
        {
            logger.LogInformation("Skipping level {Level} because it already has {Points} points", level, points);
            return;
        }

        if (levelPoints == null)
        {
            db.LevelPoints.Add(new LevelPoints
            {
                Level = level,
                Points = points
            });
        }
        else
        {
            EntityEntry<LevelPoints> entry = db.Attach(levelPoints);
            entry.Entity.Points = points;
        }

        await db.SaveChangesAsync(context.CancellationToken);
    }
}
