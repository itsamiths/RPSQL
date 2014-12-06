#dbDisconnect

setMethod(
	"dbDisconnect",
	"PSQLConnection",
	def=function(conn, ...){
		.jcall(conn@jc, "V", "close"); 
		TRUE
	}
)

#dbGetQuery

setMethod(
	"dbGetQuery",
	signature(conn="PSQLConnection", statement="character"),
	def=function(conn, statement, ...) {
		r <- dbSendQuery(conn, statement, ...)
		## Teradata needs this - closing the statement also closes the result set according to Java docs
		on.exit(.jcall(r@stat, "V", "close"))
		fetch(r, -1)
	}
)

setMethod(
	"dbReadTable",
	"PSQLConnection",
	def=function(conn, name, ...)
	dbGetQuery(conn, paste("SELECT * FROM",.sql.qescape(name,TRUE,conn@identifier.quote)))
)

#dbGetException

setMethod(
	"dbGetException",
	"PSQLConnection",
	def = function(conn, ...) list(),
	valueClass = "list"
)

#dbListResults

setMethod(
	"dbListResults",
	"PSQLConnection",
	def = function(conn, ...) {
		warning("Phoenix JDBC maintains no list of active results"); 	
		NULL 
	}
)

#dbListFields

setMethod(
	"dbListFields",
	"PSQLConnection",
	def=function(conn, name, pattern="%", full=FALSE, ...) {
		md <- .jcall(conn@jc, "Ljava/sql/DatabaseMetaData;", "getMetaData", check=FALSE)
		.verify.PSQL.result(md, "Unable to retrieve Phoenix JDBC database metadata")
		r <- .jcall(
					md,
					"Ljava/sql/ResultSet;",
					"getColumns",
					.jnull("java/lang/String"),
					.jnull("java/lang/String"),
					name,
					pattern,
					check=FALSE
			)
		.verify.PSQL.result(
			r, 	
			"Unable to retrieve Phoenix JDBC columns list for ",
			name
		)
		on.exit(.jcall(r, "V", "close"))
		ts <- character()
		while (.jcall(r, "Z", "next"))
			ts <- c(ts, .jcall(r, "S", "getString", "COLUMN_NAME"))
		.jcall(r, "V", "close")
		ts
	}
)

#dbListTables

setMethod(
	"dbListTables",
	"PSQLConnection", 
	def=function(conn, pattern="%", ...) {
		md <- .jcall(conn@jc, "Ljava/sql/DatabaseMetaData;", "getMetaData", check=FALSE)
		.verify.PSQL.result(md, "Unable to retrieve Phoenix JDBC database metadata")
		r <- .jcall(
			md,
			"Ljava/sql/ResultSet;", 
			"getTables",
			.jnull("java/lang/String"),
			.jnull("java/lang/String"),
			pattern, 
			.jnull("[Ljava/lang/String;"),
			check=FALSE
		)
		.verify.PSQL.result(r, "Unable to retrieve Phoenix JDBC tables list")
		on.exit(.jcall(r, "V", "close"))
		ts <- character()
		while (.jcall(r, "Z", "next"))
			ts <- c(ts, .jcall(r, "S", "getString", "TABLE_NAME"))
		ts
	}
)

#dbWriteTable

setMethod(
	"dbWriteTable",
	"PSQLConnection",
	def=function(conn, name, value, overwrite=TRUE, append=FALSE, ...) {
		ac <- .jcall(conn@jc, "Z", "getAutoCommit")
		overwrite <- isTRUE(as.logical(overwrite))
		append <- if (overwrite) FALSE else isTRUE(as.logical(append))
		if (is.vector(value) && !is.list(value)) 
			value <- data.frame(x=value)
		if (length(value)<1) 
			stop("value must have at least one column")
		if (is.null(names(value))) 
			names(value) <- paste("V",1:length(value),sep='')
		if (length(value[[1]])>0) {
			if (!is.data.frame(value)) 
			value <- as.data.frame(value, row.names=1:length(value[[1]]))
		} else {
			if (!is.data.frame(value)) value <- as.data.frame(value)
		}
		# to create primary key field
		index <- nrow(value) 
		value$uniqueNotNullPkField <- 1:index +1 #uniqueNotNullPkField -> pk field in newly created table
  
		fts <- sapply(value, dbDataType, dbObj=conn)
		if (dbExistsTable(conn, name)) {
			if (overwrite) dbRemoveTable(conn, name)
			else if (!append) stop("Table `",name,"' already exists")
		}
		else if (append) stop("Cannot append to a non-existing table `",name,"'")
		fdef <- paste(.sql.qescape(names(value), TRUE, conn@identifier.quote),fts,collapse=',')
		qname <- .sql.qescape(name, TRUE, conn@identifier.quote)
		if (ac) {
			.jcall(conn@jc, "V", "setAutoCommit", FALSE)
			on.exit(.jcall(conn@jc, "V", "setAutoCommit", ac))
		}
		if (!append) {
			ct <- paste("CREATE TABLE ",qname," (",fdef," CONSTRAINT pk PRIMARY KEY (uniqueNotNullPkField))",sep= '')
			dbSendUpdate(conn, ct)
		}
		if (length(value[[1]])) {
			inss <- paste("UPSERT INTO ",qname," VALUES(", paste(rep("?",length(value)),collapse=','),")",sep='')
			for (j in 1:length(value[[1]]))
				dbSendUpdate(conn, inss, list=as.list(value[j,]))
		}
		dbCommit(conn)
		#if (ac) dbCommit(conn)            
	}
)

#dbExistsTable

setMethod(
	"dbExistsTable",
	"PSQLConnection", 
	def=function(conn, name, ...) (length(dbListTables(conn, name))>0)
)

#dbRemoveTable

setMethod(
	"dbRemoveTable",
	"PSQLConnection",
	def=function(conn, name, ...) dbSendUpdate(conn, paste("DROP TABLE", name))==0
)

#dbCommit

setMethod(
	"dbCommit",
	"PSQLConnection",
	def=function(conn, ...) {
		.jcall(conn@jc, "V", "commit");
		TRUE
	}
)

#dbRollback

setMethod(
	"dbRollback",
	"PSQLConnection",
	def=function(conn, ...) {
		.jcall(conn@jc, "V", "rollback");
		TRUE
	}
)