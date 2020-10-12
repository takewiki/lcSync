library(cronR)



f <- "/home/hulilei/app/lcSync/task.R"
cmd <- cron_rscript(f)

cron_add(cmd, frequency = '*/2 * * * *', id = 'job01', description = 'sync icitem Every 2 min')   

# cron_clear(ask=FALSE)