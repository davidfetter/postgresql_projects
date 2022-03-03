/*-------------------------------------------------------------------------
 *
 * kwlist.h
 *
 * The keyword lists are kept in their own source files for use by
 * automatic tools.  The exact representation of a keyword is determined
 * by the PG_KEYWORD macro, which is not defined in this file; it can
 * be defined by the caller for special purposes.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/parser/kwlist.h
 *
 *-------------------------------------------------------------------------
 */

/* there is deliberately not an #ifndef KWLIST_H here */

/*
 * List of keyword (name, token-value, category, bare-label-status) entries.
 *
 * Note: gen_keywordlist.pl requires the entries to appear in ASCII order.
 */

/* name, value, category, is-bare-label */
PG_KEYWORD("abort", ABORT_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("absent", ABSENT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("absolute", ABSOLUTE_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("access", ACCESS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("action", ACTION, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("add", ADD_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("admin", ADMIN, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("after", AFTER, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("aggregate", AGGREGATE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("all", ALL, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("also", ALSO, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("alter", ALTER, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("always", ALWAYS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("analyse", ANALYSE, RESERVED_KEYWORD, BARE_LABEL)		/* British spelling */
PG_KEYWORD("analyze", ANALYZE, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("and", AND, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("any", ANY, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("array", ARRAY, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("as", AS, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("asc", ASC, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("asensitive", ASENSITIVE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("assertion", ASSERTION, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("assignment", ASSIGNMENT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("asymmetric", ASYMMETRIC, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("at", AT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("atomic", ATOMIC, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("attach", ATTACH, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("attribute", ATTRIBUTE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("authorization", AUTHORIZATION, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("backward", BACKWARD, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("before", BEFORE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("begin", BEGIN_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("between", BETWEEN, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("bigint", BIGINT, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("binary", BINARY, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("bit", BIT, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("boolean", BOOLEAN_P, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("both", BOTH, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("breadth", BREADTH, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("by", BY, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("cache", CACHE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("call", CALL, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("called", CALLED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("cascade", CASCADE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("cascaded", CASCADED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("case", CASE, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("cast", CAST, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("catalog", CATALOG_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("chain", CHAIN, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("char", CHAR_P, COL_NAME_KEYWORD, AS_LABEL)
PG_KEYWORD("character", CHARACTER, COL_NAME_KEYWORD, AS_LABEL)
PG_KEYWORD("characteristics", CHARACTERISTICS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("check", CHECK, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("checkpoint", CHECKPOINT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("class", CLASS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("close", CLOSE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("cluster", CLUSTER, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("coalesce", COALESCE, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("collate", COLLATE, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("collation", COLLATION, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("column", COLUMN, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("columns", COLUMNS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("comment", COMMENT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("comments", COMMENTS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("commit", COMMIT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("committed", COMMITTED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("compression", COMPRESSION, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("concurrently", CONCURRENTLY, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("conditional", CONDITIONAL, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("configuration", CONFIGURATION, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("conflict", CONFLICT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("connection", CONNECTION, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("constraint", CONSTRAINT, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("constraints", CONSTRAINTS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("content", CONTENT_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("continue", CONTINUE_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("conversion", CONVERSION_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("copy", COPY, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("cost", COST, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("create", CREATE, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("cross", CROSS, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("csv", CSV, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("cube", CUBE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("current", CURRENT_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("current_catalog", CURRENT_CATALOG, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("current_date", CURRENT_DATE, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("current_role", CURRENT_ROLE, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("current_schema", CURRENT_SCHEMA, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("current_time", CURRENT_TIME, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("current_timestamp", CURRENT_TIMESTAMP, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("current_user", CURRENT_USER, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("cursor", CURSOR, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("cycle", CYCLE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("data", DATA_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("database", DATABASE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("day", DAY_P, UNRESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("deallocate", DEALLOCATE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("dec", DEC, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("decimal", DECIMAL_P, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("declare", DECLARE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("default", DEFAULT, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("defaults", DEFAULTS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("deferrable", DEFERRABLE, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("deferred", DEFERRED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("definer", DEFINER, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("delete", DELETE_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("delimiter", DELIMITER, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("delimiters", DELIMITERS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("depends", DEPENDS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("depth", DEPTH, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("desc", DESC, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("detach", DETACH, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("dictionary", DICTIONARY, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("disable", DISABLE_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("discard", DISCARD, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("distinct", DISTINCT, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("do", DO, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("document", DOCUMENT_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("domain", DOMAIN_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("double", DOUBLE_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("drop", DROP, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("each", EACH, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("else", ELSE, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("empty", EMPTY_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("enable", ENABLE_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("encoding", ENCODING, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("encrypted", ENCRYPTED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("end", END_P, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("enum", ENUM_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("error", ERROR_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("escape", ESCAPE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("event", EVENT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("except", EXCEPT, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("exclude", EXCLUDE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("excluding", EXCLUDING, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("exclusive", EXCLUSIVE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("execute", EXECUTE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("exists", EXISTS, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("explain", EXPLAIN, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("expression", EXPRESSION, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("extension", EXTENSION, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("external", EXTERNAL, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("extract", EXTRACT, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("false", FALSE_P, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("family", FAMILY, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("fetch", FETCH, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("filter", FILTER, UNRESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("finalize", FINALIZE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("first", FIRST_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("float", FLOAT_P, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("following", FOLLOWING, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("for", FOR, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("force", FORCE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("foreign", FOREIGN, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("format", FORMAT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("forward", FORWARD, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("freeze", FREEZE, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("from", FROM, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("full", FULL, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("function", FUNCTION, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("functions", FUNCTIONS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("generated", GENERATED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("global", GLOBAL, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("grant", GRANT, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("granted", GRANTED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("greatest", GREATEST, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("group", GROUP_P, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("grouping", GROUPING, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("groups", GROUPS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("handler", HANDLER, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("having", HAVING, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("header", HEADER_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("hold", HOLD, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("hour", HOUR_P, UNRESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("identity", IDENTITY_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("if", IF_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("ilike", ILIKE, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("immediate", IMMEDIATE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("immutable", IMMUTABLE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("implicit", IMPLICIT_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("import", IMPORT_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("in", IN_P, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("include", INCLUDE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("including", INCLUDING, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("increment", INCREMENT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("index", INDEX, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("indexes", INDEXES, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("inherit", INHERIT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("inherits", INHERITS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("initially", INITIALLY, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("inline", INLINE_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("inner", INNER_P, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("inout", INOUT, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("input", INPUT_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("insensitive", INSENSITIVE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("insert", INSERT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("instead", INSTEAD, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("int", INT_P, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("integer", INTEGER, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("intersect", INTERSECT, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("interval", INTERVAL, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("into", INTO, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("invoker", INVOKER, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("is", IS, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("isnull", ISNULL, TYPE_FUNC_NAME_KEYWORD, AS_LABEL)
PG_KEYWORD("isolation", ISOLATION, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("join", JOIN, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("json", JSON, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("json_array", JSON_ARRAY, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("json_arrayagg", JSON_ARRAYAGG, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("json_exists", JSON_EXISTS, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("json_object", JSON_OBJECT, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("json_objectagg", JSON_OBJECTAGG, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("json_query", JSON_QUERY, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("json_scalar", JSON_SCALAR, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("json_serialize", JSON_SERIALIZE, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("json_value", JSON_VALUE, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("keep", KEEP, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("key", KEY, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("keys", KEYS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("label", LABEL, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("language", LANGUAGE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("large", LARGE_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("last", LAST_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("lateral", LATERAL_P, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("leading", LEADING, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("leakproof", LEAKPROOF, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("least", LEAST, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("left", LEFT, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("level", LEVEL, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("like", LIKE, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("limit", LIMIT, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("listen", LISTEN, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("load", LOAD, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("local", LOCAL, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("localtime", LOCALTIME, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("localtimestamp", LOCALTIMESTAMP, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("location", LOCATION, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("lock", LOCK_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("locked", LOCKED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("logged", LOGGED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("mapping", MAPPING, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("match", MATCH, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("matched", MATCHED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("materialized", MATERIALIZED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("maxvalue", MAXVALUE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("merge", MERGE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("method", METHOD, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("minute", MINUTE_P, UNRESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("minvalue", MINVALUE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("mode", MODE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("month", MONTH_P, UNRESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("move", MOVE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("name", NAME_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("names", NAMES, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("national", NATIONAL, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("natural", NATURAL, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("nchar", NCHAR, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("new", NEW, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("next", NEXT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("nfc", NFC, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("nfd", NFD, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("nfkc", NFKC, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("nfkd", NFKD, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("no", NO, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("none", NONE, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("normalize", NORMALIZE, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("normalized", NORMALIZED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("not", NOT, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("nothing", NOTHING, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("notify", NOTIFY, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("notnull", NOTNULL, TYPE_FUNC_NAME_KEYWORD, AS_LABEL)
PG_KEYWORD("nowait", NOWAIT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("null", NULL_P, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("nullif", NULLIF, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("nulls", NULLS_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("numeric", NUMERIC, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("object", OBJECT_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("of", OF, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("off", OFF, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("offset", OFFSET, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("oids", OIDS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("old", OLD, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("omit", OMIT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("on", ON, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("only", ONLY, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("operator", OPERATOR, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("option", OPTION, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("options", OPTIONS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("or", OR, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("order", ORDER, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("ordinality", ORDINALITY, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("others", OTHERS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("out", OUT_P, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("outer", OUTER_P, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("over", OVER, UNRESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("overlaps", OVERLAPS, TYPE_FUNC_NAME_KEYWORD, AS_LABEL)
PG_KEYWORD("overlay", OVERLAY, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("overriding", OVERRIDING, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("owned", OWNED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("owner", OWNER, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("parallel", PARALLEL, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("parser", PARSER, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("partial", PARTIAL, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("partition", PARTITION, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("passing", PASSING, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("password", PASSWORD, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("placing", PLACING, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("plans", PLANS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("policy", POLICY, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("position", POSITION, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("preceding", PRECEDING, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("precision", PRECISION, COL_NAME_KEYWORD, AS_LABEL)
PG_KEYWORD("prepare", PREPARE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("prepared", PREPARED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("preserve", PRESERVE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("primary", PRIMARY, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("prior", PRIOR, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("privileges", PRIVILEGES, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("procedural", PROCEDURAL, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("procedure", PROCEDURE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("procedures", PROCEDURES, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("program", PROGRAM, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("publication", PUBLICATION, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("quote", QUOTE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("quotes", QUOTES, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("range", RANGE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("read", READ, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("real", REAL, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("reassign", REASSIGN, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("recheck", RECHECK, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("recursive", RECURSIVE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("ref", REF, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("references", REFERENCES, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("referencing", REFERENCING, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("refresh", REFRESH, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("reindex", REINDEX, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("relative", RELATIVE_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("release", RELEASE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("rename", RENAME, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("repeatable", REPEATABLE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("replace", REPLACE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("replica", REPLICA, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("reset", RESET, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("restart", RESTART, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("restrict", RESTRICT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("return", RETURN, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("returning", RETURNING, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("returns", RETURNS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("revoke", REVOKE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("right", RIGHT, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("role", ROLE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("rollback", ROLLBACK, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("rollup", ROLLUP, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("routine", ROUTINE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("routines", ROUTINES, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("row", ROW, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("rows", ROWS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("rule", RULE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("savepoint", SAVEPOINT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("scalar", SCALAR, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("schema", SCHEMA, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("schemas", SCHEMAS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("scroll", SCROLL, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("search", SEARCH, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("second", SECOND_P, UNRESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("security", SECURITY, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("select", SELECT, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("sequence", SEQUENCE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("sequences", SEQUENCES, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("serializable", SERIALIZABLE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("server", SERVER, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("session", SESSION, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("session_user", SESSION_USER, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("set", SET, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("setof", SETOF, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("sets", SETS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("share", SHARE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("show", SHOW, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("similar", SIMILAR, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("simple", SIMPLE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("skip", SKIP, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("smallint", SMALLINT, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("snapshot", SNAPSHOT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("some", SOME, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("sql", SQL_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("stable", STABLE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("standalone", STANDALONE_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("start", START, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("statement", STATEMENT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("statistics", STATISTICS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("stdin", STDIN, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("stdout", STDOUT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("storage", STORAGE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("stored", STORED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("strict", STRICT_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("string", STRING, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("strip", STRIP_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("subscription", SUBSCRIPTION, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("substring", SUBSTRING, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("support", SUPPORT, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("symmetric", SYMMETRIC, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("sysid", SYSID, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("system", SYSTEM_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("table", TABLE, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("tables", TABLES, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("tablesample", TABLESAMPLE, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("tablespace", TABLESPACE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("temp", TEMP, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("template", TEMPLATE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("temporary", TEMPORARY, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("text", TEXT_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("then", THEN, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("ties", TIES, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("time", TIME, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("timestamp", TIMESTAMP, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("to", TO, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("trailing", TRAILING, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("transaction", TRANSACTION, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("transform", TRANSFORM, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("treat", TREAT, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("trigger", TRIGGER, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("trim", TRIM, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("true", TRUE_P, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("truncate", TRUNCATE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("trusted", TRUSTED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("type", TYPE_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("types", TYPES_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("uescape", UESCAPE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("unbounded", UNBOUNDED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("uncommitted", UNCOMMITTED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("unconditional", UNCONDITIONAL, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("unencrypted", UNENCRYPTED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("union", UNION, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("unique", UNIQUE, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("unknown", UNKNOWN, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("unlisten", UNLISTEN, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("unlogged", UNLOGGED, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("until", UNTIL, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("update", UPDATE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("user", USER, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("using", USING, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("vacuum", VACUUM, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("valid", VALID, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("validate", VALIDATE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("validator", VALIDATOR, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("value", VALUE_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("values", VALUES, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("varchar", VARCHAR, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("variadic", VARIADIC, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("varying", VARYING, UNRESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("verbose", VERBOSE, TYPE_FUNC_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("version", VERSION_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("view", VIEW, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("views", VIEWS, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("volatile", VOLATILE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("when", WHEN, RESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("where", WHERE, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("whitespace", WHITESPACE_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("window", WINDOW, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("with", WITH, RESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("within", WITHIN, UNRESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("without", WITHOUT, UNRESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("work", WORK, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("wrapper", WRAPPER, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("write", WRITE, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("xml", XML_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("xmlattributes", XMLATTRIBUTES, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("xmlconcat", XMLCONCAT, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("xmlelement", XMLELEMENT, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("xmlexists", XMLEXISTS, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("xmlforest", XMLFOREST, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("xmlnamespaces", XMLNAMESPACES, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("xmlparse", XMLPARSE, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("xmlpi", XMLPI, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("xmlroot", XMLROOT, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("xmlserialize", XMLSERIALIZE, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("xmltable", XMLTABLE, COL_NAME_KEYWORD, BARE_LABEL)
PG_KEYWORD("year", YEAR_P, UNRESERVED_KEYWORD, AS_LABEL)
PG_KEYWORD("yes", YES_P, UNRESERVED_KEYWORD, BARE_LABEL)
PG_KEYWORD("zone", ZONE, UNRESERVED_KEYWORD, BARE_LABEL)
