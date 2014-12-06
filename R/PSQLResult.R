# PSQLResult
## jr - result set, md - metadata, stat - statement
## Since the life of a result set depends on the life of the statement, we have to explicitly
## save the later as well (and close both at the end)

setClass(
	"PSQLResult",
	representation(
		"DBIResult",
		jr="jobjRef",
		md="jobjRef",
		stat="jobjRef",
		pull="jobjRef"
	)
)

setMethod(
	"fetch",
	signature(res="PSQLResult", n="numeric"),
	def=function(res, n, ...) {
		cols <- .jcall(res@md, "I", "getColumnCount")
		if (cols < 1L) return(NULL)
		l <- list()
		cts <- rep(0L, cols)
		for (i in 1:cols) {
			ct <- .jcall(res@md, "I", "getColumnType", i)
			if (ct == -5 | ct ==-6 | (ct >= 2 & ct <= 8)) {
				l[[i]] <- numeric()
				cts[i] <- 1L
			} else
				l[[i]] <- character()
			names(l)[i] <- .jcall(res@md, "S", "getColumnName", i)
		}
		rp <- res@pull
		if (is.jnull(rp)) {
			rp <- .jnew("info/itsamiths/Rpackage/RPSQL/JDBCResultPull", .jcast(res@jr, "java/sql/ResultSet"), .jarray(as.integer(cts)))
			.verify.PSQL.result(rp, "cannot instantiate JDBCResultPull hepler class")
		}
		if (n < 0L) { ## infinite pull
			stride <- 32768L  ## start fairly small to support tiny queries and increase later
			while ((nrec <- .jcall(rp, "I", "fetch", stride)) > 0L) {
				for (i in seq.int(cols))
					l[[i]] <- c(l[[i]], if (cts[i] == 1L) .jcall(rp, "[D", "getDoubles", i) else .jcall(rp, "[Ljava/lang/String;", "getStrings", i))
				if (nrec < stride) break
				stride <- 524288L # 512k
			}
		} else {
			nrec <- .jcall(rp, "I", "fetch", as.integer(n))
			for (i in seq.int(cols))
				l[[i]] <- if (cts[i] == 1L) .jcall(rp, "[D", "getDoubles", i) 
			else 
				.jcall(rp, "[Ljava/lang/String;", "getStrings", i)
		}
		# as.data.frame is expensive - create it on the fly from the list
		attr(l, "row.names") <- c(NA_integer_, length(l[[1]]))
		class(l) <- "data.frame"
		l
	}
)

## this is not needed for recent DBI, but older implementations didn't provide default methods
setMethod(
	"dbHasCompleted",
	"PSQLResult",
	def=function(res, ...) TRUE,
	valueClass="logical"
)

#dbClearResult

setMethod(
	"dbClearResult",
	"PSQLResult",
	def = function(res, ...) {
		.jcall(res@jr, "V", "close");
		.jcall(res@stat, "V", "close"); 
		TRUE
	},
	valueClass = "logical"
)

#dbColumnInfo

setMethod(
	"dbColumnInfo",
	"PSQLResult",
	def = function(res, ...) {
		cols <- .jcall(res@md, "I", "getColumnCount")
		l <- list(field.name=character(), field.type=character(), data.type=character())
		if (cols < 1) 
			return(as.data.frame(l))
		for (i in 1:cols) {
			l$field.name[i] <- .jcall(res@md, "S", "getColumnName", i)
			l$field.type[i] <- .jcall(res@md, "S", "getColumnTypeName", i)
			ct <- .jcall(res@md, "I", "getColumnType", i)
			l$data.type[i] <- if (ct == -5 | ct ==-6 | (ct >= 2 & ct <= 8)) "numeric" else "character"
		}
		as.data.frame(l, row.names=1:cols)    
	},
	valueClass = "data.frame"
)