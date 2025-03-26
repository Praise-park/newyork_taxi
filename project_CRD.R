library( rhdfs )
hdfs.init()
library( rmr2 )


################## Refine data file #########################
rmr.options( backend = 'hadoop' )
files <- hdfs.ls('/data/taxi/combined')$file

mr <- mapreduce( input = files[1],
                 input.format = make.input.format(format = 'csv', sep = ',', 
                                                  stringsAsFactors = FALSE) )
res <- from.dfs( mr )
res <- values( res )

colnames.tmp <- as.character( res[,1] )
class.tmp <- as.character( res[,2] )
colnames <- colnames.tmp[-1]
class <- class.tmp[-1]
class[c(6,8,9,10,11)] <- 'numeric'

input.format <- make.input.format(
  format = 'csv', sep = ',', stringsAsFactors = FALSE,
  col.names = colnames, colClasses = class )

files <- files[-1]

map.refine <- function( k, v ) {
  val <- v
  val <- na.omit(val)
  val <- val[ val[,15] >= 1 & val[,15] <= 6, ]  # passenger_count 1~6
  val <- val[ val[,17] > 0 & val[,17] < 100, ]  # 0 < trip_distance < 100
  val <- val[ val[,19] < 40.915 & val[,19] > 40.5 &
                val[,21] < 40.915 & val[,21] > 40.5 &
                val[,18] < -73.7 & val[,18] > -74.2 &
                val[,20] < -73.7 & val[,20] > -74.2 , ]
  
  d <- as.POSIXct( val[,13], format = "%Y-%m-%d %H:%M:%S" )
  val[,22] <- as.numeric( as.character(d, '%H') )  # hour
  weekday <- weekdays(d)
  val[,23] <- ( weekday == '토요일' | weekday == '일요일' ) # weekend
  val <- val[, -c(1:14, 16, 20, 21)]
  # passenger_count, trip_distance, pickup_longitude, pickup_latitude,
  # hour, weekend
  keyval( NULL, val )
}

data <- mapreduce( input = files, map = map.refine, input.format = input.format,
                   output = '/user/u20151073/data3' )


################### pickup place K-means ################################
dist.fun <- function( Center, Dat ) {
  apply( Center, 1, function(x) colSums( (t(Dat) - x)^2 ) )
}  

kmeans.map <- function( k, v ) {
  pickup <- v[, 3:4 ]
  D <- dist.fun( C, pickup )
  nearest <- max.col( -D ) 
  
  keyval( nearest, cbind( 1, pickup ) )
}

kmeans.reduce = function( k, v ) {
  keyval( k , t( as.matrix( apply( v, 2, sum ) ) ) )
}

### initial value of center point
C <- matrix( c( -73.994, -73.968, -73.871, -73.788, 40.734, 40.770, 40.770, 40.645 ),
             ncol = 2 )
max.iter <- 10

for( i in 1:max.iter ) {
  mr <- from.dfs( mapreduce( data, map = kmeans.map, reduce = kmeans.reduce ) )
  C <- values( mr )[,-1] / values( mr )[,1]
}
( C.count <- values( mr )[,1] )

### Add cluster column
clust.map <- function( k, v ) {
  pickup <- v[, 3:4 ]
  D <- dist.fun( C, pickup )
  cluster <- max.col( -D )
  keyval( NULL, cbind( v, cluster ) )
  # passenger_count, trip_distance, longitude, latitude, hour, weekend, cluster
}

data2 <- mapreduce( data, map = clust.map, output = '/user/u20151073/data2' )

############# Visualize clustering #######################
library(ggmap)

pickup <- (from.dfs( data2 )$val)[,c(3:4,7) ]

register_google(key = "") # Input API key

map <- get_googlemap(center = c(lon = -73.88633057230732,
                                lat = 40.70954166069161), zoom = 11)
gmap <- ggmap(map)


#### Center point
center <- data.frame(long = C[,1], lat = C[,2])
cols <- c( 'black', 'red', 'blue', 'green' )
gmap + geom_point(data = center,
                  aes(x = long, y = lat),
                  col = cols, size = 15)


#### cluster point
c1 <- pickup[pickup[,3] == 1,]
c2 <- pickup[pickup[,3] == 2,]
c3 <- pickup[pickup[,3] == 3,]
c4 <- pickup[pickup[,3] == 4,]

gmap +
  stat_density2d(data = c1[1:1000000,],
                 aes(x = pickup_longitude, y = pickup_latitude,
                     fill = "red",
                     alpha = ..level..),
                 geom = "polygon", alpha = 0.2) +
  stat_density2d(data = c2[1:1000000,],
                 aes(x = pickup_longitude, y = pickup_latitude,
                     fill = "blue",
                     alpha = ..level..),
                 geom = "polygon", alpha = 0.2) +
  stat_density2d(data = c3,
                 aes(x = pickup_longitude, y = pickup_latitude,
                     fill = "green",
                     alpha = ..level..),
                 geom = "polygon", alpha = 0.5) +
  stat_density2d(data = c4,
                 aes(x = pickup_longitude, y = pickup_latitude,
                     fill = "pink",
                     alpha = ..level..),
                 geom = "polygon", alpha = 0.5)



################## Linear model #######################
lm.map <- function( k, v ) {
  morning <- ( 7 <= v[,5] & v[,5] < 11 )
  evening <- ( 16 <= v[,5] & v[,5] < 21 )
  night <- ( 21 <= v[,5] | v[,5] < 7 )
  cluster2 <- v[,7] == 2
  cluster3 <- v[,7] == 3
  cluster4 <- v[,7] == 4
  
  Xk <- cbind( 1, cluster2, cluster3, cluster4, v[,1], v[,6], morning, evening, night )
  ## intercept, cluster, passenger_count, weekend, time
  yk <- as.matrix( v[,2] ) ## trip_distance
  
  XtXk <- crossprod( Xk, Xk )
  Xtyk <- crossprod( Xk, yk )
  ytyk <- crossprod( yk, yk )
  
  keyval( 1, list(XtXk, Xtyk, ytyk) )
}

lm.reduce <- function( k, v ) {
  XtX <- Reduce( '+', v[seq_along(v) %% 3 == 1] )
  Xty <- Reduce( '+', v[seq_along(v) %% 3 == 2] )
  yty <- Reduce( '+', v[seq_along(v) %% 3 == 0] )
  
  res <- list( XtX = XtX, Xty = Xty, yty = yty )
  keyval( 1, res )
}


lm.fit <- mapreduce( input = data2, map = lm.map, reduce = lm.reduce,
                     combine = TRUE )
lm.fit <- from.dfs( lm.fit )

fun.summary <- function( v ) {
  XtX <- v$XtX
  Xty <- v$Xty
  yty <- v$yty
  beta.hat <- solve( XtX, Xty )
  nn <- XtX[1,1]  # sample size
  
  ysum <- Xty[1]
  ybar <- ysum / nn
  stat <- list( nn = nn, beta.hat = beta.hat, ysum = ysum, ybar = ybar )
  
  SSE <- yty - crossprod(beta.hat, Xty)
  SST <- yty - ysum^2 / nn
  SSR <- SST - SSE
  SS <- list( SSR = SSR, SSE = SSE, SST = SST )
  
  df.reg <- dim(XtX)[1L] - 1
  df.tot <- nn - 1
  df.res <- df.tot - df.reg
  DF <- list( df.reg = df.reg, df.res = df.res, df.tot = df.tot )
  
  MSR <- SSR / df.reg
  MST <- SST / df.tot
  MSE <- SSE / df.res
  MS <- list( MSR = MSR, MSE = MSE, MST = MST )
  
  f.val <- MS$MSR / MS$MSE
  p.val <- pf( f.val, DF$df.reg, DF$df.res, lower.tail = FALSE )
  anova <- list( DF = DF, SS = SS, MS = MS, f.val = f.val, p.val = p.val )
  
  list( mat = v, stat = stat, anova = anova )
}

res <- fun.summary( v = lm.fit$val )
res$stat$beta.hat

################### Anova table ##########################
fun.table <- function( res ) {
  df <- as.numeric( res$anova$DF )[-3]
  ss <- as.numeric( res$anova$SS )[-3]
  ms <- as.numeric( res$anova$MS )[-3]
  f.val <- res$anova$f.val
  p.val <- res$anova$p.val
  
  table <- data.frame(df, ss, ms, f.val, p.val)
  table[2, 4:5] <- NA
  return( table )
}

table <- fun.table( res = res )

methods( print )
getAnywhere( print.anova )

dimnames( table ) <- list( c("Regressions", "Residuals"), 
                           c("Df", "Sum Sq", "Mean Sq", "F value", "Pr(>F)") )

table <- structure( table, heading = c("Analysis of Variance Table\n", 
                                       paste("Response:", "dist ~ cluster + passenger + weekend + time") ), 
                    class = c("anova", "data.frame") ) 
print( table )
