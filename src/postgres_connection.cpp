#include "postgres_connection.hpp"

#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/helper.hpp"

#include "postgres_logging.hpp"
#include "postgres_prepared_statement.hpp"

namespace duckdb {

static bool debug_postgres_print_queries = false;

PostgresConnection::PostgresConnection(PGconn *connection_p) : connection(connection_p) {
}

PostgresConnection::~PostgresConnection() {
	Close();
}

PostgresConnection::PostgresConnection(PostgresConnection &&other) noexcept {
	lock_guard<mutex> guard(other.connection_lock);
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
}

PostgresConnection &PostgresConnection::operator=(PostgresConnection &&other) noexcept {
	if (this == &other) {
		return *this;
	}
	std::lock(connection_lock, other.connection_lock);
	lock_guard<mutex> guard(connection_lock, std::adopt_lock);
	lock_guard<mutex> other_guard(other.connection_lock, std::adopt_lock);
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
	return *this;
}

PostgresConnection PostgresConnection::Open(const string &dsn, const string &attach_path) {
	PostgresConnection result;
	result.connection = PostgresUtils::PGConnect(dsn, attach_path);
	result.dsn = dsn;
	return result;
}

static bool ResultHasError(PGresult *result) {
	if (!result) {
		return true;
	}
	switch (PQresultStatus(result)) {
	case PGRES_COMMAND_OK:
	case PGRES_TUPLES_OK:
		return false;
	default:
		return true;
	}
}

PGresult *PostgresConnection::PQExecute(optional_ptr<ClientContext> context, const string &query,
                                        const PostgresParameters &params, int format) {
	if (PostgresConnection::DebugPrintQueries()) {
		Printer::Print(query + "\n");
	}
	int64_t start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                         .time_since_epoch()
	                         .count();

	PGconn *conn = GetConn();
	PGresult *res = nullptr;

	if (params.Empty() && format == 0) {
		res = PQexec(GetConn(), query.c_str());
	} else {
		// Unlike PQexec, PQexecParams allows at most one SQL command in the given string.
		res = PQexecParams(conn, query.c_str(), params.Count(), params.Types(), params.Values(), params.Lengths(),
		                   params.Formats(), format);
	}

	int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                       .time_since_epoch()
	                       .count();
	if (context) {
		DUCKDB_LOG(*context, PostgresQueryLogType, query, end_time - start_time);
	}
	return res;
}

unique_ptr<PostgresResult> PostgresConnection::TryQuery(optional_ptr<ClientContext> context, const string &query,
                                                        optional_ptr<string> error_message,
                                                        const PostgresParameters &params, int format) {
	lock_guard<mutex> guard(connection_lock);
	auto result = PQExecute(context, query.c_str(), params, format);

	if (ResultHasError(result)) {
		if (error_message) {
			auto error = PostgresUtils::ErrorMessage(GetConn(), result);
			*error_message = StringUtil::Format("Failed to execute query \"" + query + "\": " + error);
		}
		PQclear(result);
		return nullptr;
	}
	return make_uniq<PostgresResult>(result);
}

unique_ptr<PostgresResult> PostgresConnection::Query(optional_ptr<ClientContext> context, const string &query,
                                                     const PostgresParameters &params, int format) {
	string error_msg;
	auto result = TryQuery(context, query, &error_msg, params, format);
	if (!result) {
		throw std::runtime_error(error_msg);
	}
	return result;
}

unique_ptr<PostgresResult> PostgresConnection::QueryStmt(optional_ptr<ClientContext> context,
                                                         const PostgresPreparedStatement &stmt,
                                                         const PostgresParameters &params, int format) {
	auto &query = stmt.GetQuery();
	if (PostgresConnection::DebugPrintQueries()) {
		Printer::Print(query + "\n");
	}
	int64_t start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                         .time_since_epoch()
	                         .count();

	lock_guard<mutex> guard(connection_lock);
	PGconn *conn = GetConn();
	PGresult *result = PQexecPrepared(conn, stmt.GetName().c_str(), params.Count(), params.Values(), params.Lengths(),
	                                  params.Formats(), format);

	int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                       .time_since_epoch()
	                       .count();
	if (context) {
		DUCKDB_LOG(*context, PostgresQueryLogType, query, end_time - start_time);
	}

	if (ResultHasError(result)) {
		auto error = PostgresUtils::ErrorMessage(conn, result);
		auto error_message = StringUtil::Format("Failed to execute prepared statement \"%s\" for query \"%s\": %s",
		                                        stmt.GetName(), query, error);
		PQclear(result);
		throw std::runtime_error(error_message);
	}
	return make_uniq<PostgresResult>(result);
}

void PostgresConnection::Execute(optional_ptr<ClientContext> context, const string &query,
                                 const PostgresParameters &params, int format) {
	Query(context, query, params, format);
}

vector<unique_ptr<PostgresResult>> PostgresConnection::ExecuteQueries(ClientContext &context, const string &queries) {
	if (PostgresConnection::DebugPrintQueries()) {
		Printer::Print(queries + "\n");
	}
	int64_t start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                         .time_since_epoch()
	                         .count();
	lock_guard<mutex> guard(connection_lock);
	auto res = PQsendQuery(GetConn(), queries.c_str());
	if (res == 0) {
		throw std::runtime_error("Failed to execute query \"" + queries + "\": " + string(PQerrorMessage(GetConn())));
	}
	vector<unique_ptr<PostgresResult>> results;
	string error_message;
	while (true) {
		auto res = PQgetResult(GetConn());
		if (!res) {
			break;
		}
		auto result = make_uniq<PostgresResult>(res);
		if (ResultHasError(res)) {
			if (error_message.empty()) {
				error_message =
				    "Failed to execute query \"" + queries + "\": " + PostgresUtils::ErrorMessage(GetConn(), res);
			}
			continue;
		}
		if (PQresultStatus(res) != PGRES_TUPLES_OK) {
			continue;
		}
		results.push_back(std::move(result));
	}
	if (!error_message.empty()) {
		throw std::runtime_error(error_message);
	}
	int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                       .time_since_epoch()
	                       .count();
	DUCKDB_LOG(context, PostgresQueryLogType, queries, end_time - start_time);
	return results;
}

vector<PostgresPipelineResult> PostgresConnection::ExecutePipeline(ClientContext &context,
                                                                   const vector<PostgresPipelineStatement> &statements,
                                                                   bool commit, bool keep_results) {
	auto now = [] {
		return std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		    .time_since_epoch()
		    .count();
	};
	// with commit, a COMMIT follows the statements after a sync, so it runs even after a failed statement and then
	// rolls back; errors name it by its query
	PostgresPipelineStatement commit_statement {"COMMIT", PostgresParameters()};
	auto count = statements.size() + (commit ? 1 : 0);
	auto statement_at = [&](idx_t i) -> const PostgresPipelineStatement & {
		return i < statements.size() ? statements[i] : commit_statement;
	};
	auto name = [&](idx_t i) {
		return i < statements.size() ? StringUtil::Format("statement %d of the pipeline", i + 1) : string("query");
	};
	lock_guard<mutex> guard(connection_lock);
	auto conn = GetConn();
	if (!PQenterPipelineMode(conn)) {
		throw std::runtime_error("Failed to enter pipeline mode: " + string(PQerrorMessage(conn)));
	}
	auto previous = now();
	string error_message;
	idx_t sent = 0;
	for (; sent < count; sent++) {
		auto &statement = statement_at(sent);
		if (PostgresConnection::DebugPrintQueries()) {
			Printer::Print(statement.query + "\n");
		}
		auto &params = statement.params;
		auto synced = sent < statements.size() || PQpipelineSync(conn);
		// the flush request makes the server send each result as soon as its statement ran, not at the end
		if (!synced ||
		    !PQsendQueryParams(conn, statement.query.c_str(), params.Count(), params.Types(), params.Values(),
		                       params.Lengths(), params.Formats(), keep_results ? 1 : 0) ||
		    !PQsendFlushRequest(conn)) {
			error_message = StringUtil::Format("Failed to send %s \"%s\": %s", name(sent), statement.query,
			                                   string(PQerrorMessage(conn)));
			break;
		}
	}
	if (!PQpipelineSync(conn) && error_message.empty()) {
		error_message = "Failed to send pipeline: " + string(PQerrorMessage(conn));
	}
	vector<PostgresPipelineResult> results;
	// a statement the server failed makes it skip to the sync, so the COMMIT after it cannot have committed
	bool failed_before_commit = false;
	for (idx_t i = 0; i < sent; i++) {
		auto &statement = statement_at(i);
		if (i == statements.size()) {
			// the result of the sync before COMMIT
			PQclear(PQgetResult(conn));
		}
		bool executed = false;
		// the results of each statement end with a NULL
		while (auto result = PQgetResult(conn)) {
			auto status = PQresultStatus(result);
			if (status == PGRES_COPY_IN || status == PGRES_COPY_OUT || status == PGRES_COPY_BOTH) {
				PQclear(result);
				// pipeline mode does not support COPY: the connection stays in it, so the pool discards it
				throw std::runtime_error(
				    StringUtil::Format("Failed to execute %s \"%s\": COPY is not supported", name(i), statement.query));
			}
			if (status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK) {
				auto has_value =
				    !keep_results && PQntuples(result) > 0 && PQnfields(result) > 0 && !PQgetisnull(result, 0, 0);
				results.push_back({std::atoll(PQcmdTuples(result)),
				                   has_value ? Value(PQgetvalue(result, 0, 0)) : Value(LogicalType::VARCHAR)});
				if (keep_results && PQnfields(result) > 0) {
					// the caller takes the result, so it is not cleared here
					results.back().result = make_uniq<PostgresResult>(result);
					result = nullptr;
				}
				executed = true;
			} else if (status != PGRES_PIPELINE_ABORTED) {
				if (error_message.empty()) {
					error_message = StringUtil::Format("Failed to execute %s \"%s\": %s", name(i), statement.query,
					                                   PostgresUtils::ErrorMessage(conn, result));
				}
				// only errors of the server carry a SQLSTATE, not those of a lost connection
				if (i < statements.size() && PQresultErrorField(result, PG_DIAG_SQLSTATE)) {
					failed_before_commit = true;
				}
				executed = true;
			}
			PQclear(result);
		}
		auto finished = now();
		if (executed) {
			// the time since the previous result, so roughly what the server spent on the statement
			DUCKDB_LOG(context, PostgresQueryLogType, statement.query, finished - previous);
		}
		previous = finished;
	}
	// the result of the sync, which ends the pipeline
	PQclear(PQgetResult(conn));
	if (!PQexitPipelineMode(conn) && error_message.empty()) {
		error_message = "Failed to leave pipeline mode: " + string(PQerrorMessage(conn));
	}
	if (commit && results.size() == count) {
		// the transaction committed, whatever failed after its COMMIT
		results.pop_back();
		return results;
	}
	if (commit && !failed_before_commit && sent + 1 >= count && PQstatus(conn) == CONNECTION_BAD) {
		// the server may have run the COMMIT that went out
		throw std::runtime_error("Lost the connection after sending COMMIT, so whether the transaction committed is "
		                         "unknown (SQLSTATE 08007): " +
		                         error_message);
	}
	if (!error_message.empty()) {
		throw std::runtime_error(error_message);
	}
	return results;
}

PostgresVersion PostgresConnection::GetPostgresVersion(ClientContext &context) {
	auto result = TryQuery(context, "SELECT version(), (SELECT COUNT(*) FROM pg_settings WHERE name LIKE 'rds%')");
	if (!result) {
		PostgresVersion version;
		version.type_v = PostgresInstanceType::UNKNOWN;
		return version;
	}
	auto pg_version_string = result->GetString(0, 0);
	auto version = PostgresUtils::ExtractPostgresVersion(pg_version_string);
	if (result->GetInt64(0, 1) > 0) {
		version.type_v = PostgresInstanceType::AURORA;
	}
	if (StringUtil::Contains(pg_version_string, "Redshift")) {
		version.type_v = PostgresInstanceType::REDSHIFT;
	}
	return version;
}

bool PostgresConnection::IsOpen() {
	return connection != nullptr;
}

void PostgresConnection::Close() {
	lock_guard<mutex> guard(connection_lock);
	if (!connection) {
		return;
	}
	PQfinish(connection);
	connection = nullptr;
}

bool PostgresConnection::PingServer(const std::string &health_check_query) {
	lock_guard<mutex> guard(connection_lock);
	return PingServerInternal(health_check_query);
}

bool PostgresConnection::PingServerInternal(const std::string &health_check_query) {
	if (!IsOpen()) {
		return false;
	}
	PGconn *conn = GetConn();
	if (PQstatus(conn) != CONNECTION_OK) {
		return false;
	}
	if (health_check_query.empty()) {
		return true;
	}
	PGresult *res = PQexec(conn, health_check_query.c_str());
	PostgresResult res_holder(res);
	return PQresultStatus(res) == PGRES_TUPLES_OK;
}

void PostgresConnection::Reset(const std::string &health_check_query) {
	lock_guard<mutex> guard(connection_lock);
	if (!IsOpen()) {
		throw InternalException("Cannot reset a connection that is not open");
	}
	PGconn *conn = GetConn();
	auto tx_status = PQtransactionStatus(conn);
	if (tx_status == PQTRANS_INTRANS || tx_status == PQTRANS_INERROR) {
		PGresult *res = PQexec(conn, "ROLLBACK");
		PostgresResult res_holder(res);
	}
	{
		PGresult *res = PQexec(conn, "DISCARD ALL");
		PostgresResult res_holder(res);
		if (PQresultStatus(res) == PGRES_COMMAND_OK) {
			return;
		}
	}
	PQreset(conn);
	if (!PingServerInternal(health_check_query)) {
		throw InternalException("Connection reset failure");
	}
}

vector<IndexInfo> PostgresConnection::GetIndexInfo(const string &table_name) {
	return vector<IndexInfo>();
}

void PostgresConnection::DebugSetPrintQueries(bool print) {
	debug_postgres_print_queries = print;
}

bool PostgresConnection::DebugPrintQueries() {
	return debug_postgres_print_queries;
}

} // namespace duckdb
