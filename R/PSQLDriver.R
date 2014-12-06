#PSQLDriver

setClass(
	"PSQLDriver",
	representation(
		"DBIDriver",
		identifier.quote="character",
		psqlDriver="jobjRef"
	)
)

PSQL <- function(driverClass='', classPath='', identifier.quote=NA) {
	## expand all paths in the classPath
	classPath <- path.expand(unlist(strsplit(classPath, .Platform$path.sep)))
	.jinit(classPath) ## this is benign in that it's equivalent to .jaddClassPath if a JVM is running
	.jaddClassPath(system.file("java", "RPSQL.jar", package="RPSQL"))
	if (nchar(driverClass) && is.jnull(.jfindClass(as.character(driverClass)[1])))
		stop("Cannot find Phoenix driver class ",driverClass)
	psqlDriver <- .jnew(driverClass, check=FALSE)
	.jcheck(TRUE)
	if (is.jnull(psqlDriver)) 
		psqlDriver <- .jnull()
	new("PSQLDriver", identifier.quote=as.character(identifier.quote), psqlDriver=psqlDriver)
}

#dbGetInfo

setMethod(
	"dbGetInfo",
	"PSQLDriver",
	def=function(dbObj, ...)
	list(
		name="PSQL", 
		driver.version="0.1-1",
		DBI.version="0.1-1",
		client.version=NA,
		max.connections=NA
	)
)

setMethod(
	"dbGetInfo",
	"PSQLConnection",
	def = function(dbObj, ...)
	list() 
)

setMethod(
	"dbGetInfo",
	"PSQLResult",
	def=function(dbObj, ...)
	list(has.completed=TRUE),
	valueClass="list"
)

#dbConnect

setMethod(
	"dbConnect",
	"PSQLDriver",
	def=function(drv, url, user='', password='', ...) {
		jc <- .jcall("java/sql/DriverManager","Ljava/sql/Connection;","getConnection", as.character(url)[1], as.character(user)[1], as.character(password)[1], check=FALSE)
		if (is.jnull(jc) && !is.jnull(drv@psqlDriver)) {
			# ok one reason for this to fail is its interaction with rJava's
			# class loader. In that case we try to load the driver directly.
			oex <- .jgetEx(TRUE)
			p <- .jnew("java/util/Properties")
			if (length(user)==1 && nchar(user))
				.jcall(p,"Ljava/lang/Object;","setProperty","user",user)
			if (length(password)==1 && nchar(password))
				.jcall(p,"Ljava/lang/Object;","setProperty","password",password)
			l <- list(...)
			if (length(names(l)))
				for (n in names(l))
					.jcall(p, "Ljava/lang/Object;", "setProperty", n, as.character(l[[n]]))
			jc <- .jcall(drv@psqlDriver, "Ljava/sql/Connection;", "connect", as.character(url)[1], p)
		}
		.verify.PSQL.result(jc, "Unable to connect Phoenix JDBC to ",url)
		new("PSQLConnection", jc=jc, identifier.quote=drv@identifier.quote)
	},
	valueClass="PSQLConnection"
)

#dbUnloadDriver

setMethod(
	"dbUnloadDriver",
	"PSQLDriver",
	def=function(drv, ...) 
	FALSE
)

#dbListConnections

setMethod(
	"dbListConnections",
	"PSQLDriver",
	def=function(drv, ...) {
		warning("Phoenix JDBC driver maintains no list of active connections."); 
		list() 
	}
)

#dbDataType

setMethod(
	"dbDataType",
	signature(dbObj="PSQLConnection", obj = "ANY"),
	def = function(dbObj, obj, ...) {
		if (is.integer(obj)) "INTEGER"
		else if (is.numeric(obj)) "DOUBLE"
		else "VARCHAR"
	},
	valueClass = "character"
)