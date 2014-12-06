# PSQLConnection

setClass(
	"PSQLConnection",
	representation(
		"DBIConnection",
		jc="jobjRef",
		identifier.quote="character"
	)
)

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

#dbGetFields

if (is.null(getGeneric("dbGetFields")))
	setGeneric(
		"dbGetFields",
		function(conn, ...) 
		standardGeneric("dbGetFields")
	)

setMethod(
	"dbGetFields",
	"PSQLConnection",
	def=function(conn, name, pattern="%", ...) {
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
		.verify.PSQL.result(r, "Unable to retrieve Phoenix JDBC columns list for ",name)
		on.exit(.jcall(r, "V", "close"))
		.fetch.result(r)
	}
)

#dbGetTables

if (is.null(getGeneric("dbGetTables"))) 
	setGeneric(
		"dbGetTables",
		function(conn, ...) 
		standardGeneric("dbGetTables")
	)

setMethod(
	"dbGetTables",
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
		.fetch.result(r)
	}
)

#dbSendQuery

setMethod(
	"dbSendQuery",
	signature(conn="PSQLConnection", statement="character"),
	def=function(conn, statement, ..., list=NULL) {
		statement <- as.character(statement)[1L]
		## if the statement starts with {call or {?= call then we use CallableStatement 
		if (isTRUE(as.logical(grepl("^\\{(call|\\?= *call)", statement)))) {
			s <- .jcall(conn@jc, "Ljava/sql/CallableStatement;", "prepareCall", statement, check=FALSE)
			.verify.PSQL.result(s, "Unable to execute Phoenix JDBC callable statement ",statement)
			if (length(list(...)))
				.fillStatementParameters(s, list(...))
			if (!is.null(list))
				.fillStatementParameters(s, list)
			r <- .jcall(s, "Ljava/sql/ResultSet;", "executeQuery", check=FALSE)
			.verify.PSQL.result(r, "Unable to retrieve Phoenix JDBC result set for ",statement)
		} else if (length(list(...)) || length(list)) { ## use prepared statements if there are additional arguments
			s <- .jcall(conn@jc, "Ljava/sql/PreparedStatement;", "prepareStatement", statement, check=FALSE)
			.verify.PSQL.result(s, "Unable to execute Phoenix JDBC prepared statement ", statement)
			if (length(list(...)))
				.fillStatementParameters(s, list(...))
			if (!is.null(list))
				.fillStatementParameters(s, list)
			r <- .jcall(s, "Ljava/sql/ResultSet;", "executeQuery", check=FALSE)
			.verify.PSQL.result(r, "Unable to retrieve Phoenix JDBC result set for ",statement)
		} else { 
			s <- .jcall(conn@jc, "Ljava/sql/Statement;", "createStatement")
			.verify.PSQL.result(s, "Unable to create simple Phoenix JDBC statement ",statement)
			r <- .jcall(s, "Ljava/sql/ResultSet;", "executeQuery", as.character(statement)[1], check=FALSE)
			.verify.PSQL.result(r, "Unable to retrieve Phoenix JDBC result set for ",statement)
		} 
		md <- .jcall(r, "Ljava/sql/ResultSetMetaData;", "getMetaData", check=FALSE)
		.verify.PSQL.result(md, "Unable to retrieve Phoenix JDBC result set meta data for ",statement, " in dbSendQuery")
		new("PSQLResult", jr=r, md=md, stat=s, pull=.jnull())
	}
)

#dbSendUpdate

if (is.null(getGeneric("dbSendUpdate")))
	setGeneric(
		"dbSendUpdate",
		function(conn, statement, ...)
		standardGeneric("dbSendUpdate")
	)

setMethod(
	"dbSendUpdate",
	signature(conn="PSQLConnection", statement="character"),
	def=function(conn, statement, ..., list=NULL) {
		statement <- as.character(statement)[1L]
		## if the statement starts with {call or {?= call then we use CallableStatement 
		if (isTRUE(as.logical(grepl("^\\{(call|\\?= *call)", statement)))) {
			s <- .jcall(conn@jc, "Ljava/sql/CallableStatement;", "prepareCall", statement, check=FALSE)
			.verify.PSQL.result(s, "Unable to execute Phoenix JDBC callable statement ",statement)
			on.exit(.jcall(s, "V", "close")) 
			if (length(list(...)))
				.fillStatementParameters(s, list(...))
			if (!is.null(list))
				.fillStatementParameters(s, list)
			r <- .jcall(s, "Ljava/sql/ResultSet;", "executeQuery", check=FALSE)
			.verify.PSQL.result(r, "Unable to retrieve Phoenix JDBC result set for ",statement)
		} else if (length(list(...)) || length(list)) { ## use prepared statements if there are additional arguments
			s <- .jcall(conn@jc, "Ljava/sql/PreparedStatement;", "prepareStatement", statement, check=FALSE)
			.verify.PSQL.result(s, "Unable to execute Phoenix JDBC prepared statement ", statement)
			on.exit(.jcall(s, "V", "close")) 
			if (length(list(...)))
				.fillStatementParameters(s, list(...))
			if (!is.null(list))
				.fillStatementParameters(s, list)
			.jcall(s, "I", "executeUpdate", check=FALSE)
		} else {
			s <- .jcall(conn@jc, "Ljava/sql/Statement;", "createStatement")
			.verify.PSQL.result(s, "Unable to create Phoenix JDBC statement ",statement)
			on.exit(.jcall(s, "V", "close")) 
			.jcall(s, "I", "executeUpdate", as.character(statement)[1], check=FALSE)
		}
		x <- .jgetEx(TRUE)
		if (!is.jnull(x))
			stop("execute Phoenix JDBC update query failed in dbSendUpdate (", .jcall(x, "S", "getMessage"),")")
	}
)