#include "duckdb.hpp"

#include "dbconnector/defer.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
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
	return std::move(result);
}

static PostgresQueryMode ResolveQueryMode(ClientContext &context, TableFunctionBindInput &input,
                                          const vector<Value> &params) {
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
	if (params.size() > 0) {
		return PostgresQueryMode::PLAIN_QUERY_BINARY;
	}
	return PostgresQueryMode::AUTO;
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

//! Parses and describes a query as the unnamed statement, in one round trip together with the statements deferred
//! on the connection. The unnamed statement is not closed, the next statement replaces it
static PGresult *DescribeUnnamed(PostgresConnection &con, const string &sql) {
	auto conn = con.GetConn();
	if (PostgresConnection::DebugPrintQueries()) {
		Printer::Print("DESCRIBE: " + sql + "\n");
	}
	auto deferred = con.TakeDeferred();
	bool sent = PQenterPipelineMode(conn);
	for (auto &statement : deferred) {
		sent = sent && PQsendQueryParams(conn, statement.c_str(), 0, nullptr, nullptr, nullptr, nullptr, 0);
	}
	if (!sent || !PQsendPrepare(conn, "", sql.c_str(), 0, nullptr) || !PQsendDescribePrepared(conn, "") ||
	    !PQpipelineSync(conn)) {
		throw BinderException("Failed to prepare query \"%s\": %s", sql, PQerrorMessage(conn));
	}
	PGresult *describe = nullptr;
	string error;
	auto describe_command = deferred.size() + 1;
	for (idx_t command = 0; command <= describe_command; command++) {
		// the results of each command end with a NULL
		while (auto result = PQgetResult(conn)) {
			auto status = PQresultStatus(result);
			if (status == PGRES_FATAL_ERROR && error.empty()) {
				error = PQresultErrorMessage(result);
			}
			if (command == describe_command && status == PGRES_COMMAND_OK) {
				describe = result;
			} else {
				PQclear(result);
			}
		}
	}
	// the result of the sync, which ends the pipeline
	PQclear(PQgetResult(conn));
	PQexitPipelineMode(conn);
	if (!error.empty() || !describe) {
		PQclear(describe);
		throw BinderException("Failed to prepare query \"%s\": %s", sql, error.empty() ? PQerrorMessage(conn) : error);
	}
	return describe;
}

//! Prepares a query as a named statement, which a plain query can execute later
static unique_ptr<PostgresPreparedStatement> PrepareNamed(PostgresConnection &con, uint64_t pool_connection_id,
                                                          const string &sql) {
	auto conn_lock = con.Lock();
	auto conn = con.GetConn();
	string stmt_name = PostgresPreparedStatement::GenerateName();
	if (PostgresConnection::DebugPrintQueries()) {
		Printer::Print("PREPARE " + stmt_name + ": " + sql + "\n");
	}
	// a named statement stays in the session until it is closed
	con.NoteSessionChanged();
	PGresult *prepared_res_ptr = PQprepare(conn, stmt_name.c_str(), sql.c_str(), 0, nullptr);
	return make_uniq<PostgresPreparedStatement>(conn, pool_connection_id, stmt_name, sql, prepared_res_ptr);
}

//! Whether pg_execute_in_bind runs the query at bind, which it does for queries but not for the commands of a pushdown
static bool ExecuteInBindByDefault(ClientContext &context, TableFunctionBindInput &input) {
	Value setting;
	if (!context.TryGetCurrentSetting("pg_execute_in_bind", setting) || setting.IsNull() ||
	    !BooleanValue::Get(setting)) {
		return false;
	}
	for (auto command_flag : {"return_changed_rows", "suppress_dml_output"}) {
		if (input.named_parameters.find(Identifier(command_flag)) != input.named_parameters.end()) {
			return false;
		}
	}
	return true;
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

	if (!ExtractFlag(input, "prepare", true)) {
		return BindDML(context, input, return_types, names, catalog, con, std::move(preliminary_queries),
		               std::move(sql), use_transaction, pinned_connection_id,
		               PostgresParameters::Text(std::move(param_values)), nullptr);
	}

	// the result of the query, when bind runs it instead of describing it
	unique_ptr<PostgresResult> bind_result;
	auto explicit_execute_in_bind = input.named_parameters.find(Identifier("execute_in_bind")) != input.named_parameters.end();
	auto execute_in_bind = ExtractFlag(input, "execute_in_bind", ExecuteInBindByDefault(context, input));
	auto conn_lock = con.Lock();
	// the result types and names of the query
	PostgresResult describe_wrapper(execute_in_bind ? nullptr : DescribeUnnamed(con, sql));
	conn_lock.unlock();
	if (execute_in_bind) {
		bind_result = con.Query(context, sql, PostgresParameters::Text(param_values), 1);
		if (PQnfields(bind_result->res) <= 0) {
			if (explicit_execute_in_bind) {
				throw BinderException("execute_in_bind requires a query that returns columns: \"%s\"", sql);
			}
			// a command that ran already, the scan returns its row count
			auto dml = BindDML(context, input, return_types, names, catalog, con, std::move(preliminary_queries),
			                   sql, use_transaction, pinned_connection_id,
			                   PostgresParameters::Text(std::move(param_values)), nullptr);
			dml->Cast<PostgresBindData>().bind_result = std::move(bind_result);
			return dml;
		}
	}
	auto describe_prepared = execute_in_bind ? bind_result->res : describe_wrapper.res;
	int nparams = execute_in_bind ? NumericCast<int>(param_values.size()) : PQnparams(describe_prepared);
	if (nparams != param_values.size()) {
		throw BinderException("Incorrect number of parameters specified, expected: %d, actual: %zu, query: \"%s\"",
		                      nparams, param_values.size(), sql);
	}
	vector<Oid> param_types;
	for (idx_t p = 0; p < nparams; p++) {
		// without a description the server infers the types, as for the text parameters it ran the query with
		Oid ptype = execute_in_bind ? 0 : PQparamtype(describe_prepared, p);
		param_types.emplace_back(ptype);
	}
	int nfields = PQnfields(describe_prepared);
	if (nfields <= 0) {
		// a command is sent with its parameters, it does not execute a prepared statement
		return BindDML(context, input, return_types, names, catalog, con, std::move(preliminary_queries),
		               std::move(sql), use_transaction, pinned_connection_id,
		               PostgresParameters(std::move(param_types), std::move(param_values)), nullptr);
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
	result->query_mode = ResolveQueryMode(context, input, param_values);
	result->params = execute_in_bind ? PostgresParameters::Text(std::move(param_values))
	                                 : PostgresParameters(std::move(param_types), std::move(param_values));
	result->use_transaction = use_transaction;
	result->clear_catalog_cache = ExtractFlag(input, "clear_catalog_cache", false);
	PostgresScanFunction::PrepareBind(catalog.GetPostgresVersion(), context, *result, 0, catalog);
	if (bind_result) {
		// the scan reads the result instead of running the query again
		result->query_mode = PostgresQueryMode::PLAIN_QUERY_BINARY;
		result->bind_result = std::move(bind_result);
		return std::move(result);
	}
	// a plain query can execute a statement prepared here, a COPY sends the query itself
	if (result->query_mode == PostgresQueryMode::PLAIN_QUERY_BINARY || result->use_text_protocol) {
		result->prepared_statement = PrepareNamed(con, pool_connection_id, result->sql);
	}
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
		    options.Add("execute_in_bind", LogicalType::BOOLEAN);
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
