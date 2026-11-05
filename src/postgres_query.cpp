#include "duckdb.hpp"

#include "dbconnector/defer.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "postgres_parameters.hpp"
#include "postgres_prepared_statement.hpp"
#include "postgres_scanner.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"

namespace duckdb {

static bool ExtractFlag(TableFunctionBindInput &input, const string &name, bool default_val) {
	auto it = input.named_parameters.find(Identifier(name));
	if (it != input.named_parameters.end()) {
		Value &bool_val = it->second;
		if (!bool_val.IsNull()) {
			return BooleanValue::Get(bool_val);
		}
	}
	return default_val;
}

static void RegisterDatabaseAccess(ClientContext &context, TableFunctionBindInput &input, PostgresCatalog &catalog) {
	// "modifies_database" is set for queries and statements that are executed remotely in place of DuckDB queries /
	// statements - arbitrary postgres_query / postgres_execute calls are not registered, as their prepared Postgres
	// statements are reused across executions, and it is unknown whether they write to the database
	auto it = input.named_parameters.find(Identifier("modifies_database"));
	if (!input.binder || it == input.named_parameters.end() || it->second.IsNull()) {
		return;
	}
	// let DuckDB check the transaction (e.g. that a read-only transaction does not write, or that a transaction only
	// writes to a single database), and rebind prepared statements when the catalog may have changed
	auto &properties = input.binder->GetStatementProperties();
	properties.RegisterDBRead(catalog, context);
	if (BooleanValue::Get(it->second)) {
		properties.RegisterDBModify(catalog, context, DatabaseModificationType::UPDATE_DATA);
	}
}

static unique_ptr<FunctionData> BindDML(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<Identifier> &names,
                                        PostgresCatalog &catalog, PostgresConnection &con,
                                        vector<string> preliminary_queries, std::string sql, bool use_transaction,
                                        uint64_t pinned_connection_id, PostgresParameters params,
                                        unique_ptr<PostgresPreparedStatement> prepared_statement) {
	// The statement returns no result columns: it's a command (DDL, or DML without RETURNING).
	// Instead of failing, run it as a command and return a single-row Success result. We reuse
	// the prepare/describe just done — no extra round-trip — and defer execution to
	// InitGlobalState (execution time, not bind, so EXPLAIN does not run it).
	auto result = make_uniq<PostgresBindData>(context, catalog.GetName());
	result->command_only = true;
	result->clear_catalog_cache = ExtractFlag(input, "clear_catalog_cache", false);
	result->pinned_connection_id = pinned_connection_id;
	if (ExtractFlag(input, "suppress_dml_output", false)) {
		// This invocation wraps a command with no result set (DDL, or DML without RETURNING). Tell the
		// binder via the return-type modifier so that when this is routed through CONNECT the outer
		// statement is reported as NOTHING and displays like a native command (no spurious result table).
		input.table_function.call_return_type = StatementReturnType::NOTHING;
	} else if (ExtractFlag(input, "return_changed_rows", false)) {
		// The command is a DML statement that is executed remotely in place of a DuckDB statement - report the
		// number of changed rows, like DuckDB reports them for its own DML statements
		input.table_function.call_return_type = StatementReturnType::CHANGED_ROWS;
	}
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back(Identifier("rowcount"));
	result->dsn = con.GetDSN();
	result->types = return_types;
	result->names.emplace_back(names[0].GetIdentifierName());
	result->read_only = false;
	result->preliminary_queries = std::move(preliminary_queries);
	result->sql = std::move(sql);
	result->params = std::move(params);
	result->use_transaction = use_transaction;
	result->prepared_statement = std::move(prepared_statement);
	PostgresScanFunction::PrepareBind(catalog.GetPostgresVersion(), context, *result, 0, catalog);
	RegisterDatabaseAccess(context, input, catalog);
	return std::move(result);
}

static PostgresQueryMode ResolveQueryMode(ClientContext &context, TableFunctionBindInput &input, bool has_params) {
	auto it = input.named_parameters.find(Identifier("query_mode"));
	if (it != input.named_parameters.end()) {
		Value &str_val = it->second;
		if (!str_val.IsNull()) {
			if (str_val.type().id() != LogicalTypeId::VARCHAR) {
				throw BinderException("Invalid non-VARCHAR 'query_mode' parameter specified");
			}
			PostgresQueryMode mode = PostgresUtils::ExtractQueryMode(str_val);
			if (mode != PostgresQueryMode::AUTO) {
				return mode;
			}
		}
	}
	PostgresQueryMode mode = PostgresUtils::ExtractQueryMode(context);
	if (mode != PostgresQueryMode::AUTO) {
		return mode;
	}
	Value text_proto_val;
	if (context.TryGetCurrentSetting("pg_use_text_protocol", text_proto_val) && !text_proto_val.IsNull() &&
	    BooleanValue::Get(text_proto_val)) {
		return PostgresQueryMode::PLAIN_QUERY_TEXT;
	}
	if (has_params) {
		return PostgresQueryMode::PLAIN_QUERY_BINARY;
	}
	return PostgresQueryMode::AUTO;
}

struct QueryBindDescription {
	unique_ptr<PostgresPreparedStatement> prepared;
	//! Either the PQdescribePrepared result or the result of running the query in bind
	unique_ptr<PostgresResult> result;
	PostgresParameters params;
};

static QueryBindDescription ExecuteQueryInBind(ClientContext &context, PostgresConnection &con, const string &sql,
                                               vector<Value> param_values, const vector<string> &preliminary_queries) {
	if (!preliminary_queries.empty()) {
		throw BinderException("execute_in_bind cannot be combined with preliminary_queries");
	}
	QueryBindDescription desc;
	desc.params = PostgresParameters::Text(std::move(param_values));
	string error_message;
	// the result describes the query, so that it is not prepared and described in a round trip of its own
	desc.result = con.TryQuery(context, sql, &error_message, desc.params, 1);
	if (!desc.result) {
		throw BinderException(error_message);
	}
	if (PQnfields(desc.result->res) <= 0) {
		throw BinderException("execute_in_bind requires a query that returns columns");
	}
	return desc;
}

static QueryBindDescription DescribePreparedQuery(PostgresConnection &con, uint64_t pool_connection_id,
                                                  const string &sql, vector<Value> param_values) {
	QueryBindDescription desc;
	auto conn_lock = con.Lock();
	auto conn = con.GetConn();
	// prepare execution of the query to figure out the result types and names
	string stmt_name = PostgresPreparedStatement::GenerateName();
	if (PostgresConnection::DebugPrintQueries()) {
		Printer::Print("PREPARE " + stmt_name + ": " + sql + "\n");
	}
	PGresult *prepared_res_ptr = PQprepare(conn, stmt_name.c_str(), sql.c_str(), 0, nullptr);
	desc.prepared = make_uniq<PostgresPreparedStatement>(conn, pool_connection_id, stmt_name, sql, prepared_res_ptr);
	// use describe_prepared
	desc.result = make_uniq<PostgresResult>(PQdescribePrepared(conn, desc.prepared->GetName().c_str()));
	auto describe_prepared = desc.result->res;
	if (!describe_prepared || PQresultStatus(describe_prepared) != PGRES_COMMAND_OK) {
		auto extended_err = PostgresUtils::GetErrorMessage(describe_prepared, conn);
		throw BinderException("Failed to describe prepared statement: %s", extended_err);
	}
	conn_lock.unlock();

	int nparams = PQnparams(describe_prepared);
	if (nparams != param_values.size()) {
		throw BinderException("Incorrect number of parameters specified, expected: %d, actual: %zu", nparams,
		                      param_values.size());
	}
	vector<Oid> param_types;
	param_types.reserve(nparams);
	for (idx_t p = 0; p < nparams; p++) {
		param_types.emplace_back(PQparamtype(describe_prepared, p));
	}
	desc.params = PostgresParameters(std::move(param_types), std::move(param_values));
	return desc;
}

static string StripTrailingSemicolon(string sql) {
	StringUtil::RTrim(sql);
	while (!sql.empty() && sql.back() == ';') {
		sql = sql.substr(0, sql.size() - 1);
		StringUtil::RTrim(sql);
	}
	return sql;
}

static vector<Value> ExtractParams(TableFunctionBindInput &input) {
	auto params_it = input.named_parameters.find("params");
	if (params_it != input.named_parameters.end()) {
		Value &struct_val = params_it->second;
		if (struct_val.IsNull()) {
			throw BinderException("Parameters to postgres_query cannot be NULL");
		}
		if (struct_val.type().id() != LogicalTypeId::STRUCT && struct_val.type().id() != LogicalTypeId::TUPLE) {
			throw BinderException("Query parameters must be specified in a STRUCT");
		}
		return StructValue::GetChildren(struct_val);
	}
	return vector<Value>();
}

static vector<string> ExtractPreliminaryQueries(TableFunctionBindInput &input) {
	vector<string> queries;
	auto it = input.named_parameters.find("preliminary_queries");
	if (it != input.named_parameters.end()) {
		Value &list_val = it->second;
		if (list_val.IsNull()) {
			throw BinderException("Specified preliminary queries LIST must be not NULL");
		}
		if (list_val.type().id() != LogicalTypeId::LIST) {
			throw BinderException("Preliminary queries must be specified in a LIST");
		}
		for (const Value &val : ListValue::GetChildren(list_val)) {
			if (val.IsNull()) {
				throw BinderException("All preliminary queries must be not NULL");
			}
			if (val.type().id() != LogicalTypeId::VARCHAR) {
				throw BinderException("All preliminary queries must be of type VARCHAR");
			}
			queries.push_back(StringValue::Get(val));
		}
	}
	return queries;
}

static uint64_t ExtractPinnedConnId(TableFunctionBindInput &input) {
	uint64_t pinned_connection_id = 0;
	auto conn_it = input.named_parameters.find("connection");
	if (conn_it != input.named_parameters.end()) {
		Value &conn_val = conn_it->second;
		if (conn_val.IsNull()) {
			throw BinderException("Specified connection must be not null");
		}
		pinned_connection_id = UBigIntValue::Get(conn_val);
	}
	return pinned_connection_id;
}

static unique_ptr<FunctionData> PGQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("Parameters to postgres_query cannot be NULL");
	}

	auto db_name = input.inputs[0].GetValue<string>();
	auto &catalog = PostgresUtils::GetCatalogByName(context, db_name);
	auto input_sql = input.inputs[1].GetValue<string>();
	auto sql = StripTrailingSemicolon(std::move(input_sql));
	bool use_transaction = ExtractFlag(input, "use_transaction", true);
	vector<Value> param_values = ExtractParams(input);
	vector<string> preliminary_queries = ExtractPreliminaryQueries(input);
	uint64_t pinned_connection_id = ExtractPinnedConnId(input);

	optional_ptr<PostgresConnection> con_ptr = nullptr;
	PostgresPoolConnection pinned_connection;
	uint64_t pool_connection_id = 0;

	if (pinned_connection_id > 0) {
		pinned_connection = catalog.GetConnectionPool().UnpinConnection(pinned_connection_id);
		PostgresConnection &conn = pinned_connection.GetConnection();
		con_ptr = &conn;
		pool_connection_id = pinned_connection.Id();
	} else {
		auto &transaction = Transaction::Get(context, catalog).Cast<PostgresTransaction>();
		if (use_transaction) {
			con_ptr = &transaction.GetConnection();
		} else {
			con_ptr = &transaction.GetConnectionWithoutTransaction();
		}
		pool_connection_id = transaction.GetPoolConnectionId();
	}

	auto deferred_pin = dbconnector::Defer([&pinned_connection, &catalog] {
		if (!pinned_connection) {
			return;
		}
		try {
			catalog.GetConnectionPool().PinConnection(std::move(pinned_connection));
		} catch (...) {
			// suppress
		}
	});

	PostgresConnection &con = *con_ptr;

	auto execute_in_bind = ExtractFlag(input, "execute_in_bind", false);
	if (!ExtractFlag(input, "prepare", true)) {
		if (execute_in_bind) {
			throw BinderException("execute_in_bind cannot be combined with prepare=FALSE");
		}
		return BindDML(context, input, return_types, names, catalog, con, std::move(preliminary_queries),
		               std::move(sql), use_transaction, pinned_connection_id,
		               PostgresParameters::Text(std::move(param_values)), nullptr);
	}

	auto query_desc = execute_in_bind
	                      ? ExecuteQueryInBind(context, con, sql, std::move(param_values), preliminary_queries)
	                      : DescribePreparedQuery(con, pool_connection_id, sql, std::move(param_values));
	auto describe_prepared = query_desc.result->res;
	int nfields = PQnfields(describe_prepared);
	if (nfields <= 0) {
		return BindDML(context, input, return_types, names, catalog, con, std::move(preliminary_queries),
		               std::move(sql), use_transaction, pinned_connection_id, std::move(query_desc.params),
		               std::move(query_desc.prepared));
	}
	auto result = make_uniq<PostgresBindData>(context, catalog.GetName());
	result->pinned_connection_id = pinned_connection_id;
	auto type_config = PostgresTypeConfig::FromContext(context);
	for (idx_t c = 0; c < nfields; c++) {
		PostgresType postgres_type;
		postgres_type.oid = PQftype(describe_prepared, c);
		PostgresTypeData type_data;
		type_data.instance_type = catalog.GetPostgresVersion().type_v;
		if (type_data.instance_type == PostgresInstanceType::REDSHIFT) {
			type_data.type_name = PostgresUtils::RedshiftOidToName(postgres_type.oid);
		}
		if (type_data.type_name.empty()) {
			type_data.type_name = PostgresUtils::PostgresOidToName(postgres_type.oid);
		}
		type_data.type_modifier = PQfmod(describe_prepared, c);
		auto converted_type = PostgresUtils::TypeToLogicalType(nullptr, nullptr, type_config, type_data, postgres_type);
		result->postgres_types.push_back(postgres_type);
		return_types.emplace_back(converted_type);
		char *name_ptr = PQfname(describe_prepared, c);
		string name = name_ptr ? name_ptr : "unnamed";
		names.emplace_back(name);
	}
	PostgresUtils::DeduplicateColumns(names);

	// set up the bind data
	result->type_config = type_config;
	result->dsn = con.GetDSN();
	result->types = return_types;
	for (auto &nm : names) {
		result->names.emplace_back(nm.GetIdentifierName());
	}
	result->read_only = false;
	result->preliminary_queries = std::move(preliminary_queries);
	result->sql = std::move(sql);
	result->params = std::move(query_desc.params);
	result->use_transaction = use_transaction;
	result->clear_catalog_cache = ExtractFlag(input, "clear_catalog_cache", false);
	if (execute_in_bind) {
		for (idx_t c = 0; c < result->postgres_types.size(); c++) {
			auto &postgres_type = result->postgres_types[c];
			if (ContainsCastToVarchar(postgres_type) ||
			    postgres_type.info == PostgresTypeAnnotation::TEXT_PROTOCOL_ONLY) {
				// the scan query would cast such a column to VARCHAR, but the scan reads the result that bind got
				throw BinderException(
				    "Column \"%s\" has a type that cannot be read in the binary format, cast it to TEXT in the query",
				    result->names[c]);
			}
		}
		// the scan reads the result instead of running the query again, in the binary format it has
		result->query_mode = PostgresQueryMode::PLAIN_QUERY_BINARY;
		result->execute_in_bind = true;
		result->bind_result = std::move(query_desc.result);
		result->bind_query = context.transaction.GetActiveQuery();
	} else {
		result->query_mode = ResolveQueryMode(context, input, !result->params.Empty());
		result->prepared_statement = std::move(query_desc.prepared);
	}
	PostgresScanFunction::PrepareBind(catalog.GetPostgresVersion(), context, *result, 0, catalog);
	RegisterDatabaseAccess(context, input, catalog);
	return std::move(result);
}

static InsertionOrderPreservingMap<string> PostgresQueryToString(TableFunctionToStringInput &input) {
	D_ASSERT(input.bind_data);
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<PostgresBindData>();
	if (!bind_data.preliminary_queries.empty()) {
		result["Preliminary Queries"] = StringUtil::Join(bind_data.preliminary_queries, "\n");
	}
	result["SQL"] = bind_data.sql;
	return result;
}

static FunctionSignature PostgresQuerySignature(bool suppress_dml_output) {
	FunctionSignature signature;
	signature.AddParameter("database", LogicalType::VARCHAR)
	    .AddParameter("sql", LogicalType::VARCHAR)
	    .WithTypedKwargs("options", [&](TypedKwargs &options) {
		    options.Add("use_transaction", LogicalType::BOOLEAN).Add("params", LogicalType::ANY);
		    if (suppress_dml_output) {
			    options.Add("suppress_dml_output", LogicalType::BOOLEAN);
		    }
		    options.Add("prepare", LogicalType::BOOLEAN);
		    options.Add("query_mode", LogicalType::VARCHAR);
		    options.Add("connection", LogicalType::UBIGINT);
		    options.Add("preliminary_queries", LogicalType::LIST(LogicalType::VARCHAR));
		    options.Add("clear_catalog_cache", LogicalType::BOOLEAN);
		    options.Add("return_changed_rows", LogicalType::BOOLEAN);
		    options.Add("modifies_database", LogicalType::BOOLEAN);
		    if (suppress_dml_output) {
			    // runs at bind, so EXPLAIN runs it too; 1 round trip instead of prepare, describe and the read
			    options.Add("execute_in_bind", LogicalType::BOOLEAN);
		    }
	    });
	return signature;
}

PostgresQueryFunction::PostgresQueryFunction()
    : TableFunction("postgres_query", PostgresQuerySignature(true), nullptr, PGQueryBind) {
	PostgresScanFunction scan_function;
	init_global = scan_function.init_global;
	init_local = scan_function.init_local;
	function = scan_function.function;
	to_string = PostgresQueryToString;
	projection_pushdown = true;
	global_initialization = TableFunctionInitialization::INITIALIZE_ON_SCHEDULE;
}

PostgresExecuteFunction::PostgresExecuteFunction()
    : TableFunction("postgres_execute", PostgresQuerySignature(false), nullptr, PGQueryBind) {
	PostgresScanFunction scan_function;
	init_global = scan_function.init_global;
	init_local = scan_function.init_local;
	function = scan_function.function;
	projection_pushdown = true;
	global_initialization = TableFunctionInitialization::INITIALIZE_ON_SCHEDULE;
}
} // namespace duckdb
