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