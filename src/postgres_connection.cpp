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

#include <algorithm>

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
	std::swap(session_changed, other.session_changed);
	std::swap(open_statements, other.open_statements);
	std::swap(only_reads, other.only_reads);
	std::swap(commit_pending, other.commit_pending);
	std::swap(dsn, other.dsn);
	std::swap(last_answer, other.last_answer);
}

PostgresConnection &PostgresConnection::operator=(PostgresConnection &&other) noexcept {
	if (this == &other) {
		return *this;
	}
	std::lock(connection_lock, other.connection_lock);
	lock_guard<mutex> guard(connection_lock, std::adopt_lock);
	lock_guard<mutex> other_guard(other.connection_lock, std::adopt_lock);
	std::swap(connection, other.connection);
	std::swap(session_changed, other.session_changed);
	std::swap(open_statements, other.open_statements);
	std::swap(only_reads, other.only_reads);
	std::swap(commit_pending, other.commit_pending);
	std::swap(dsn, other.dsn);
	std::swap(last_answer, other.last_answer);
	return *this;
}

PostgresConnection PostgresConnection::Open(const string &dsn, const string &attach_path) {
	PostgresConnection result;
	result.connection = PostgresUtils::PGConnect(dsn, attach_path);
	result.dsn = dsn;
	result.MarkAnswered();
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

//! Runs the statements and then the query with its parameters in one pipeline, in one round trip. Returns the result
//! of the first statement that failed, or else that of the query
static PGresult *ExecuteAfter(PGconn *conn, const vector<string> &statements, const string &query,
                              const PostgresParameters &params, int format) {
	if (!PQenterPipelineMode(conn)) {
		return nullptr;
	}
	auto sent = std::all_of(statements.begin(), statements.end(), [&](const string &statement) {
		return PQsendQueryParams(conn, statement.c_str(), 0, nullptr, nullptr, nullptr, nullptr, 0);
	});
	sent = sent && PQsendQueryParams(conn, query.c_str(), params.Count(), params.Types(), params.Values(),
	                                 params.Lengths(), params.Formats(), format);
	sent = PQpipelineSync(conn) && sent;
	PGresult *result = nullptr;
	for (idx_t command = 0; sent && command <= statements.size(); command++) {
		// the results of each command end with a NULL
		while (auto command_result = PQgetResult(conn)) {
			auto status = PQresultStatus(command_result);
			if (!result && (status == PGRES_FATAL_ERROR || command == statements.size())) {
				result = command_result;
			} else {
				PQclear(command_result);
			}
		}
	}
	// the result of the sync, which ends the pipeline
	PQclear(PQgetResult(conn));
	PQexitPipelineMode(conn);
	return result;
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
	ReadPendingCommit();
	// the log shows the deferred statements that go with the query
	auto sent_query = query;

	if (params.Empty() && format == 0) {
		res = PQexec(GetConn(), query.c_str());
	} else if (deferred.empty()) {
		// Unlike PQexec, PQexecParams allows at most one SQL command in the given string.
		res = PQexecParams(conn, query.c_str(), params.Count(), params.Types(), params.Values(), params.Lengths(),
		                   params.Formats(), format);
	} else {
		// so the deferred statements, such as BEGIN, go ahead of the query in a pipeline, in the same round trip
		auto statements = TakeDeferred();
		sent_query = StringUtil::Join(statements, ";\n") + ";\n" + query;
		res = ExecuteAfter(conn, statements, query, params, format);
	}
	NoteStatements(sent_query);

	int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                       .time_since_epoch()
	                       .count();
	MarkAnswered();
	if (context) {
		DUCKDB_LOG(*context, PostgresQueryLogType, sent_query, end_time - start_time);
	}
	return res;
}

unique_ptr<PostgresResult> PostgresConnection::TryQuery(optional_ptr<ClientContext> context, const string &query,
                                                        optional_ptr<string> error_message,
                                                        const PostgresParameters &params, int format) {
	lock_guard<mutex> guard(connection_lock);
	auto sent_query = query;
	if (params.Empty() && format == 0) {
		// a plain query can hold several statements, so the deferred ones go with it and the error names them too
		sent_query = PrependDeferred(query);
	}
	auto result = PQExecute(context, sent_query, params, format);

	if (ResultHasError(result)) {
		if (error_message) {
			auto error = PostgresUtils::ErrorMessage(GetConn(), result);
			*error_message = StringUtil::Format("Failed to execute query \"" + sent_query + "\": " + error);
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
	ExecuteDeferred(context);
	PGconn *conn = GetConn();
	NoteStatements(query);
	PGresult *result = PQexecPrepared(conn, stmt.GetName().c_str(), params.Count(), params.Values(), params.Lengths(),
	                                  params.Formats(), format);
	MarkAnswered();

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
	auto sent_queries = PrependDeferred(queries);
	NoteStatements(sent_queries);
	ReadPendingCommit();
	auto res = PQsendQuery(GetConn(), sent_queries.c_str());
	if (res == 0) {
		throw std::runtime_error("Failed to execute query \"" + sent_queries +
		                         "\": " + string(PQerrorMessage(GetConn())));
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
				    "Failed to execute query \"" + sent_queries + "\": " + PostgresUtils::ErrorMessage(GetConn(), res);
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
	MarkAnswered();
	DUCKDB_LOG(context, PostgresQueryLogType, sent_queries, end_time - start_time);
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
	// pipeline mode needs an idle connection
	ReadPendingCommit();
	if (!PQenterPipelineMode(conn)) {
		throw std::runtime_error("Failed to enter pipeline mode: " + string(PQerrorMessage(conn)));
	}
	auto previous = now();
	string error_message;
	// the deferred statements, such as BEGIN, go ahead of the statements in the same round trip
	auto ahead = TakeDeferred();
	for (auto &statement : ahead) {
		NoteStatements(statement);
		if (error_message.empty() &&
		    !PQsendQueryParams(conn, statement.c_str(), 0, nullptr, nullptr, nullptr, nullptr, 0)) {
			error_message =
			    StringUtil::Format("Failed to send query \"%s\": %s", statement, string(PQerrorMessage(conn)));
		}
	}
	idx_t sent = 0;
	for (; error_message.empty() && sent < count; sent++) {
		auto &statement = statement_at(sent);
		NoteStatements(statement.query);
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
	// a statement the server failed makes it skip to the sync, so the COMMIT after it cannot have committed
	bool failed_before_commit = false;
	for (auto &statement : ahead) {
		while (auto result = PQgetResult(conn)) {
			if (error_message.empty() && ResultHasError(result) && PQresultStatus(result) != PGRES_PIPELINE_ABORTED) {
				error_message = StringUtil::Format("Failed to execute query \"%s\": %s", statement,
				                                   PostgresUtils::ErrorMessage(conn, result));
			}
			if (PQresultErrorField(result, PG_DIAG_SQLSTATE)) {
				failed_before_commit = true;
			}
			PQclear(result);
		}
	}
	// the log shows the deferred statements with the first statement, as it does for a plain query that carries them
	auto logged_ahead = ahead.empty() ? string() : StringUtil::Join(ahead, ";\n") + ";\n";
	vector<PostgresPipelineResult> results;
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
			DUCKDB_LOG(context, PostgresQueryLogType, (i == 0 ? logged_ahead : string()) + statement.query,
			           finished - previous);
		}
		previous = finished;
	}
	// the result of the sync, which ends the pipeline
	PQclear(PQgetResult(conn));
	MarkAnswered();
	if (!PQexitPipelineMode(conn) && error_message.empty()) {
		error_message = "Failed to leave pipeline mode: " + string(PQerrorMessage(conn));
	}
	if (commit && !failed_before_commit && results.size() == count) {
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

vector<unique_ptr<PostgresResult>> PostgresConnection::TryQueries(optional_ptr<ClientContext> context,
                                                                  const vector<string> &queries) {
	auto all_queries = StringUtil::Join(queries, ";\n");
	if (PostgresConnection::DebugPrintQueries()) {
		Printer::Print(all_queries + "\n");
	}
	int64_t start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                         .time_since_epoch()
	                         .count();
	lock_guard<mutex> guard(connection_lock);
	auto conn = GetConn();
	// pipeline mode needs an idle connection
	ReadPendingCommit();
	NoteStatements(all_queries);
	if (!PQenterPipelineMode(conn)) {
		throw std::runtime_error("Failed to execute query \"" + all_queries + "\": " + string(PQerrorMessage(conn)));
	}
	// a sync after each query runs it in a transaction of its own, so that its error affects no other query
	for (auto &query : queries) {
		PQsendQueryParams(conn, query.c_str(), 0, nullptr, nullptr, nullptr, nullptr, 0);
		PQsendPipelineSync(conn);
	}
	PQflush(conn);
	vector<unique_ptr<PostgresResult>> results;
	for (idx_t i = 0; i < queries.size(); i++) {
		// the result of the query, the end of its results and the result of the sync
		auto result = make_uniq<PostgresResult>(PQgetResult(conn));
		PQclear(PQgetResult(conn));
		PQclear(PQgetResult(conn));
		results.push_back(ResultHasError(result->res) ? nullptr : std::move(result));
	}
	PQexitPipelineMode(conn);
	MarkAnswered();
	int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                       .time_since_epoch()
	                       .count();
	if (context) {
		DUCKDB_LOG(*context, PostgresQueryLogType, all_queries, end_time - start_time);
	}
	return results;
}

PostgresVersion PostgresConnection::GetPostgresVersion(ClientContext &context) {
	return GetPostgresVersion(TryQuery(context, GetPostgresVersionQuery()).get());
}

string PostgresConnection::GetPostgresVersionQuery() {
	return "SELECT version(), (SELECT COUNT(*) FROM pg_settings WHERE name LIKE 'rds%')";
}

PostgresVersion PostgresConnection::GetPostgresVersion(optional_ptr<PostgresResult> result) {
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

void PostgresConnection::MarkAnswered() {
	last_answer = std::chrono::steady_clock::now();
}

bool PostgresConnection::AnsweredWithin(uint64_t milliseconds) const {
	return std::chrono::steady_clock::now() - last_answer < std::chrono::milliseconds(milliseconds);
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

void PostgresConnection::Defer(string statement) {
	lock_guard<mutex> guard(connection_lock);
	deferred.push_back(std::move(statement));
}

void PostgresConnection::DeferBegin(string begin) {
	lock_guard<mutex> guard(connection_lock);
	deferred.push_back(std::move(begin));
	begin_deferred = true;
	only_reads = true;
}

bool PostgresConnection::CancelBegin() {
	lock_guard<mutex> guard(connection_lock);
	if (!begin_deferred || deferred.size() > 1) {
		return false;
	}
	deferred.clear();
	begin_deferred = false;
	return true;
}

string PostgresConnection::PrependDeferred(const string &query) {
	deferred.push_back(query);
	auto result = StringUtil::Join(deferred, ";\n");
	deferred.clear();
	begin_deferred = false;
	return result;
}

void PostgresConnection::Commit(optional_ptr<ClientContext> context, bool reads_without_waiting) {
	{
		lock_guard<mutex> guard(connection_lock);
		if (reads_without_waiting && only_reads && deferred.empty()) {
			// nothing was written, so whatever the reply says, the transaction is over
			commit_pending = PQsendQuery(GetConn(), "COMMIT");
			if (context) {
				DUCKDB_LOG(*context, PostgresQueryLogType, "COMMIT", 0);
			}
			return;
		}
	}
	Execute(context, "COMMIT");
}

void PostgresConnection::ReadPendingCommit() {
	if (!commit_pending) {
		return;
	}
	commit_pending = false;
	auto conn = GetConn();
	while (auto result = PQgetResult(conn)) {
		// a failed COMMIT of reads ended the transaction all the same
		if (ResultHasError(result) && DebugPrintQueries()) {
			Printer::Print("COMMIT sent without waiting failed: " + string(PQresultErrorMessage(result)));
		}
		PQclear(result);
	}
	// a lost connection is replaced before the next request, as the pool replaces one that fails its health check
	PQconsumeInput(conn);
	if (PQstatus(conn) == CONNECTION_BAD) {
		PQreset(conn);
	}
}

vector<string> PostgresConnection::TakeDeferred() {
	vector<string> result;
	std::swap(result, deferred);
	begin_deferred = false;
	return result;
}

void PostgresConnection::ExecuteDeferred(optional_ptr<ClientContext> context) {
	// raw requests such as a prepare follow, so the reply to a COMMIT is read here too
	ReadPendingCommit();
	if (deferred.empty()) {
		return;
	}
	auto statements = StringUtil::Join(deferred, ";\n");
	deferred.clear();
	begin_deferred = false;
	PostgresResult result(PQExecute(context, statements));
	if (ResultHasError(result.res)) {
		auto error = result.res ? PQresultErrorMessage(result.res) : PQerrorMessage(GetConn());
		throw std::runtime_error("Failed to execute query \"" + statements + "\": " + string(error));
	}
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
	ReadPendingCommit();
	PGresult *res = PQexec(conn, health_check_query.c_str());
	PostgresResult res_holder(res);
	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		return false;
	}
	MarkAnswered();
	return true;
}

void PostgresConnection::Reset(const std::string &health_check_query) {
	lock_guard<mutex> guard(connection_lock);
	if (!IsOpen()) {
		throw InternalException("Cannot reset a connection that is not open");
	}
	PGconn *conn = GetConn();
	deferred.clear();
	begin_deferred = false;
	auto tx_status = PQtransactionStatus(conn);
	if (tx_status == PQTRANS_INTRANS || tx_status == PQTRANS_INERROR) {
		PGresult *res = PQexec(conn, "ROLLBACK");
		PostgresResult res_holder(res);
	}
	// a COMMIT sent without waiting has ended the transaction, the next request reads its reply
	if (!session_changed && open_statements == 0 && (commit_pending || PQtransactionStatus(conn) == PQTRANS_IDLE)) {
		return;
	}
	ReadPendingCommit();
	session_changed = false;
	open_statements = 0;
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

//! The words of a text in upper case, split at anything but letters, digits and underscores
static vector<string> Words(const string &text) {
	vector<string> words;
	idx_t start = 0;
	for (idx_t i = 0; i <= text.size(); i++) {
		if (i < text.size() && (StringUtil::CharacterIsAlphaNumeric(text[i]) || text[i] == '_')) {
			continue;
		}
		if (i > start) {
			words.push_back(StringUtil::Upper(text.substr(start, i - start)));
		}
		start = i + 1;
	}
	return words;
}

//! Whether the statements of a query leave the session as it was, so that DISCARD ALL has nothing to undo: no
//! settings, temporary objects, session advisory locks, cursors or prepared statements that outlive the transaction.
//! Anything unknown counts as a change. Literals and comments are not skipped: a semicolon or a word in them can only
//! make a query count as a change, never hide one.
static bool LeavesSessionUnchanged(const string &sql) {
	// statements whose effects on the session end with the transaction
	static const unordered_set<string> NEUTRAL = {
	    "SELECT", "WITH",    "INSERT",   "UPDATE", "DELETE", "MERGE",   "COPY",      "BEGIN",  "START", "COMMIT",
	    "END",    "ABORT",   "ROLLBACK", "VALUES", "TABLE",  "EXPLAIN", "CREATE",    "ALTER",  "DROP",  "LOCK",
	    "VACUUM", "ANALYZE", "TRUNCATE", "GRANT",  "REVOKE", "COMMENT", "SAVEPOINT", "RELEASE"};
	// words that change the session wherever they appear, as in SELECT ... INTO TEMP or SELECT set_config(...)
	static const unordered_set<string> CHANGING = {"TEMP",
	                                               "TEMPORARY",
	                                               "PG_TEMP",
	                                               "SET_CONFIG",
	                                               "PG_ADVISORY_LOCK",
	                                               "PG_ADVISORY_LOCK_SHARED",
	                                               "PG_TRY_ADVISORY_LOCK",
	                                               "PG_TRY_ADVISORY_LOCK_SHARED"};
	for (auto &statement : StringUtil::Split(sql, ';')) {
		auto start = statement.find_first_not_of(" \t\n\r\f\v");
		if (start == string::npos) {
			continue;
		}
		// a statement that starts with a comment or a parenthesis hides its kind
		if (!StringUtil::CharacterIsAlpha(statement[start])) {
			return false;
		}
		auto words = Words(statement);
		// SET LOCAL and SET TRANSACTION end with the transaction
		auto set_local = words[0] == "SET" && words.size() > 1 && (words[1] == "LOCAL" || words[1] == "TRANSACTION");
		if (!set_local && NEUTRAL.count(words[0]) == 0) {
			return false;
		}
		if (std::any_of(words.begin(), words.end(), [](const string &word) { return CHANGING.count(word) > 0; })) {
			return false;
		}
	}
	return true;
}

//! Whether the statements of a query only read, so that a failed COMMIT would lose nothing. As above, literals and
//! comments can only make a query count as a write. A function that writes is not detected.
static bool OnlyReads(const string &sql) {
	static const unordered_set<string> READING = {"BEGIN", "SET", "SHOW", "SELECT", "WITH", "VALUES", "TABLE"};
	// also SELECT ... INTO, which creates a table, SELECT ... FOR UPDATE, which locks rows, and SERIALIZABLE in a BEGIN
	// or SET TRANSACTION: such a COMMIT can fail to serialize, and then the reads must not be used
	static const unordered_set<string> WRITING = {"INSERT", "UPDATE", "DELETE", "MERGE", "INTO", "SERIALIZABLE"};
	auto statements = StringUtil::Split(sql, ';');
	return std::all_of(statements.begin(), statements.end(), [](const string &statement) {
		auto words = Words(statement);
		if (words.empty()) {
			return true;
		}
		// a COPY reads when it copies to the client, as scans do
		auto reads =
		    words[0] == "COPY" ? std::count(words.begin(), words.end(), "STDOUT") > 0 : READING.count(words[0]) > 0;
		return reads &&
		       std::none_of(words.begin(), words.end(), [](const string &word) { return WRITING.count(word) > 0; });
	});
}

void PostgresConnection::NoteStatements(const string &sql) {
	session_changed = session_changed || !LeavesSessionUnchanged(sql);
	only_reads = only_reads && OnlyReads(sql);
}

void PostgresConnection::NoteStatementPrepared() {
	open_statements++;
}

void PostgresConnection::NoteStatementClosed() {
	open_statements--;
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
