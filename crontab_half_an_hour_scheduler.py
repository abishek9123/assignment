from crontab import CronTab

cron = CronTab(user='root')
job = cron.new(command='duplicate_removal.py')
job.minute.every(30)

cron.write()