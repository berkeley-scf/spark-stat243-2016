data <- read.csv('obama-counts/part-00000')
names(data) <- c('day', 'hour', 'traffic')

day <- data$day - 20081100
hour <- data$hour / 10000

datetime <- day*24 + hour

datetime <- datetime - 5  # convert to eastern US time
datetime <- datetime / 24

# order from earliest to latest date-times
ord <- order(datetime)

pdf('obama-counts/obama-traffic.pdf')
plot(datetime[ord], data$traffic[ord], type = 'l',
     xlab = 'day in November, 2008', ylab = 'hits')

abline(v = 5, col = 'red')
dev.off()
