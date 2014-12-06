.verify.PSQL.result <- function (result, ...) {
	if (is.jnull(result)) {
		x <- .jgetEx(TRUE)
		if (is.jnull(x))
			stop(...)
		else
			stop(...," (",.jcall(x, "S", "getMessage"),")")
  }
}

.fillStatementParameters <- function(s, l) {
	for (i in 1:length(l)) {
		v <- l[[i]]
		if (is.na(v)) { 
			sqlType <- if (is.integer(v)) 4 else if (is.numeric(v)) 8 else 12
			.jcall(s, "V", "setNull", i, as.integer(sqlType))
		} else if (is.integer(v))
			.jcall(s, "V", "setInt", i, v[1])
		else if (is.numeric(v))
			.jcall(s, "V", "setDouble", i, as.double(v)[1])
		else
			.jcall(s, "V", "setString", i, as.character(v)[1])
  }
}

.sql.qescape <- function(s, identifier=FALSE, quote="\"") {
	s <- as.character(s)
	if (identifier) {
		vid <- grep("^[A-Za-z]+([A-Za-z0-9_]*)$",s)
		if (length(s[-vid])) {
			if (is.na(quote))
				stop("The Phoenix JDBC connection doesn't support quoted identifiers, but table/column name contains characters that must be quoted (",paste(s[-vid],collapse=','),")")
			s[-vid] <- .sql.qescape(s[-vid], FALSE, quote)
		}
    return(s)
	}
	if (is.na(quote))
		quote <- ''
	s <- gsub("\\\\","\\\\\\\\",s)
	if (nchar(quote))
		s <- gsub(paste("\\",quote,sep=''),paste("\\\\\\",quote,sep=''),s,perl=TRUE)
	paste(quote,s,quote,sep='')
}

.fetch.result <- function(r) {
	md <- .jcall(r, "Ljava/sql/ResultSetMetaData;", "getMetaData", check=FALSE)
	.verify.PSQL.result(md, "Unable to retrieve Phoenix JDBC result set meta data")
	res <- new("PSQLResult", jr=r, md=md, stat=.jnull(), pull=.jnull())
	fetch(res, -1)
}
