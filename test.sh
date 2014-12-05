#!/bin/sh

## Poor man's test suite -- if you want to run it,
## you'll need Apache phoenix on the local machine and
## adjust the paths/names below as needed

PSQL=/home/ubuntu/phoenix-4.2.1-bin
DRIVER="$PSQL/phoenix-4.2.1-client.jar"
DRVCLASS=org.apache.phoenix.jdbc.PhoenixDriver

##---- cut here ----

if [ ! -e "$DRIVER" ]; then
    echo "Cannot find JDBC driver for Apache Phoenix, cannot perform PSQL tests"
    exit 1
fi

echo "library(RPSQL)
## load driver
drv<-JDBC('$DRVCLASS','$DRIVER',identifier.quote="'")
## connect
c<-dbConnect(drv, "jdbc:phoenix:hbase", "", "")
## table operations write/read
data(mtcars)
dbWriteTable(c, "rpsql.mtcars", mtcars, overwrite=TRUE)
dbReadTable(c, "rpsql.mtcars")
## simple send query
fetch(r <- dbSendQuery(c, "SELECT count(*) FROM rpsql.mtcars"), 1)
## query info - some people need this
dbGetInfo(r)
dbHasCompleted(r)
## prepared send query
fetch(dbSendQuery(c, "SELECT MPG, count(MPG) FROM rpsql.mtcars WHERE CYL  > ? GROUP BY MPG", 3), 1e3)
## simple update
dbSendUpdate(c, "DROP TABLE IF EXISTS foo")
dbSendUpdate(c, "CREATE TABLE foo (alpha VARCHAR(32), beta INT)")
## prepared update
dbSendUpdate(c, "INSERT INTO foo VALUES (?, ?)", "foo", 123)
dbSendUpdate(c, "INSERT INTO foo VALUES (?, ?)", "bar", 456)
fetch(dbSendQuery(c, "SELECT * FROM foo"), -1)
## calls
dbSendUpdate(c, "CREATE PROCEDURE bar() BEGIN SELECT * FROM foo; END")
fetch(dbSendQuery(c, "{call bar()}"), -1)
dbSendUpdate(c, "DROP PROCEDURE bar")
## parametrized
dbSendUpdate(c, "CREATE PROCEDURE foobar(IN x INT) BEGIN SELECT * FROM foo WHERE beta >= x; END")
fetch(dbSendQuery(c, "{call foobar(222)}"), -1)
dbSendUpdate(c, "DROP PROCEDURE foobar")

dbDisconnect(c)
' | R --vanilla --quiet || echo "*** TEST FAILED ***"

