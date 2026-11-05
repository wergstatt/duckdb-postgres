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

//! The error of a result with its SQLSTATE, by which callers can tell errors apart in any server language
static string ResultErrorMessage(PGresult *result) {
	string message = PQresultErrorMessage(result);
	StringUtil::RTrim(message);
	auto sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
	if (sqlstate) {
		message += string(" (SQLSTATE ") + sqlstate + ")";
	}
	return message;
}

PostgresConnection::PostgresConnection(PGconn *connection_p) : connection(connection_p) {
}

PostgresConnection::~PostgresConnection() {
	Close();
}

PostgresConnection::PostgresConnection(PostgresConnection &&other) noexcept {
	lock_guard<mutex> guard(other.connection_lock);
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
	std::swap(last_answer, other.last_answer);
	std::swap(session_changed, other.session_changed);
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
	std::swap(last_answer, other.last_answer);
	std::swap(session_changed, other.session_changed);
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

//! Runs the statements and then the query with its parameters, in one round trip. Returns the result of the first
//! statement that failed, or else that of the query
static PGresult *ExecuteAfter(PGconn *conn, const vector<string> &statements, const string &query,
                              const PostgresParameters &params, int format) {
	if (!PQenterPipelineMode(conn)) {
		return nullptr;
	}
	bool sent = true;
	for (auto &statement : statements) {
		sent = sent && PQsendQueryParams(conn, statement.c_str(), 0, nullptr, nullptr, nullptr, nullptr, 0);
	}
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
	NoteStatements(query);
	auto deferred_statements = TakeDeferred();
	// the log shows what is sent, deferred statements included
	auto sent_query = deferred_statements.empty() ? query : StringUtil::Join(deferred_statements, ";\n") + ";\n" + query;

	if (params.Empty() && format == 0) {
		// a query string can hold several statements, so the deferred ones go ahead of it in the same round trip
		deferred_statements.push_back(query);
		res = PQexec(GetConn(), StringUtil::Join(deferred_statements, ";\n").c_str());
	} else {
		// Unlike PQexec, PQexecParams allows at most one SQL command in the given string.
		res = deferred_statements.empty()
		          ? PQexecParams(conn, query.c_str(), params.Count(), params.Types(), params.Values(),
		                         params.Lengths(), params.Formats(), format)
		          : ExecuteAfter(conn, deferred_statements, query, params, format);
	}

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
	auto result = PQExecute(context, query.c_str(), params, format);

	if (ResultHasError(result)) {
		if (error_message) {
			auto error = result ? ResultErrorMessage(result) : string(PQerrorMessage(GetConn()));
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
	NoteStatements(query);
	auto deferred_statements = TakeDeferred();
	if (!deferred_statements.empty()) {
		PostgresResult deferred_result(PQexec(conn, StringUtil::Join(deferred_statements, ";\n").c_str()));
		if (ResultHasError(deferred_result.res)) {
			throw std::runtime_error("Failed to execute query \"" + StringUtil::Join(deferred_statements, ";\n") +
			                         "\": " + ResultErrorMessage(deferred_result.res));
		}
	}
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
		auto error = result ? ResultErrorMessage(result) : string(PQerrorMessage(conn));
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
	NoteStatements(queries);
	auto all_queries = TakeDeferred();
	all_queries.push_back(queries);
	auto sent_queries = StringUtil::Join(all_queries, ";\n");
	auto res = PQsendQuery(GetConn(), sent_queries.c_str());
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
				error_message = "Failed to execute query \"" + queries + "\": " + ResultErrorMessage(res);
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

vector<PostgresPipelineResult>
PostgresConnection::ExecutePipeline(ClientContext &context, const vector<PostgresPipelineStatement> &requested) {
	auto now = [] {
		return std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		    .time_since_epoch()
		    .count();
	};
	lock_guard<mutex> guard(connection_lock);
	vector<PostgresPipelineStatement> statements;
	for (auto &statement : TakeDeferred()) {
		statements.push_back({std::move(statement), PostgresParameters()});
	}
	auto deferred_count = statements.size();
	statements.insert(statements.end(), requested.begin(), requested.end());
	auto conn = GetConn();
	if (!PQenterPipelineMode(conn)) {
		throw std::runtime_error("Failed to enter pipeline mode: " + string(PQerrorMessage(conn)));
	}
	auto previous = now();
	string error_message;
	idx_t sent = 0;
	for (auto &statement : statements) {
		if (PostgresConnection::DebugPrintQueries()) {
			Printer::Print(statement.query + "\n");
		}
		NoteStatements(statement.query);
		auto &params = statement.params;
		// the flush request makes the server send each result as soon as its statement ran, not at the end
		if (!PQsendQueryParams(conn, statement.query.c_str(), params.Count(), params.Types(), params.Values(),
		                       params.Lengths(), params.Formats(), 0) ||
		    !PQsendFlushRequest(conn)) {
			error_message = "Failed to send query \"" + statement.query + "\": " + string(PQerrorMessage(conn));
			break;
		}
		sent++;
	}
	if (!PQpipelineSync(conn) && error_message.empty()) {
		error_message = "Failed to send pipeline: " + string(PQerrorMessage(conn));
	}
	vector<PostgresPipelineResult> results;
	for (idx_t i = 0; i < sent; i++) {
		bool executed = false;
		// the results of each statement end with a NULL
		while (auto result = PQgetResult(conn)) {
			auto status = PQresultStatus(result);
			if (status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK) {
				auto has_value = PQntuples(result) > 0 && PQnfields(result) > 0 && !PQgetisnull(result, 0, 0);
				results.push_back({std::atoll(PQcmdTuples(result)),
				                   has_value ? Value(PQgetvalue(result, 0, 0)) : Value(LogicalType::VARCHAR)});
				executed = true;
			} else if (status != PGRES_PIPELINE_ABORTED) {
				if (error_message.empty()) {
					error_message =
					    "Failed to execute query \"" + statements[i].query + "\": " + ResultErrorMessage(result);
				}
				executed = true;
			}
			PQclear(result);
		}
		auto finished = now();
		if (executed) {
			// the time since the previous result, so roughly what the server spent on the statement
			DUCKDB_LOG(context, PostgresQueryLogType, statements[i].query, finished - previous);
		}
		previous = finished;
	}
	// the result of the sync, which ends the pipeline
	PQclear(PQgetResult(conn));
	MarkAnswered();
	if (!PQexitPipelineMode(conn) && error_message.empty()) {
		error_message = "Failed to leave pipeline mode: " + string(PQerrorMessage(conn));
	}
	if (!error_message.empty()) {
		throw std::runtime_error(error_message);
	}
	// the deferred statements were not requested by the caller
	results.erase(results.begin(), results.begin() + MinValue<idx_t>(deferred_count, results.size()));
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

//! The words that start the statements of a query, skipping literals, quoted identifiers and comments
static vector<vector<string>> LeadingWords(const string &sql, idx_t word_count) {
	vector<vector<string>> result {{}};
	idx_t i = 0;
	while (i < sql.size()) {
		auto c = sql[i];
		if (c == ';') {
			result.emplace_back();
			i++;
		} else if (c == '\'' || c == '"') {
			auto end = sql.find(c, i + 1);
			while (end != string::npos && end + 1 < sql.size() && sql[end + 1] == c) {
				end = sql.find(c, end + 2);
			}
			i = end == string::npos ? sql.size() : end + 1;
		} else if (c == '-' && i + 1 < sql.size() && sql[i + 1] == '-') {
			auto end = sql.find('\n', i);
			i = end == string::npos ? sql.size() : end + 1;
		} else if (c == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
			auto end = sql.find("*/", i + 2);
			i = end == string::npos ? sql.size() : end + 2;
		} else if (c == '$') {
			auto tag_end = sql.find('$', i + 1);
			auto tag = tag_end == string::npos ? string() : sql.substr(i, tag_end - i + 1);
			auto is_tag = !tag.empty() && std::all_of(tag.begin() + 1, tag.end() - 1, [](char t) {
				return StringUtil::CharacterIsAlphaNumeric(t) || t == '_';
			});
			if (!is_tag || StringUtil::CharacterIsDigit(tag[1])) {
				i++;
				continue;
			}
			auto end = sql.find(tag, tag_end + 1);
			i = end == string::npos ? sql.size() : end + tag.size();
		} else if (StringUtil::CharacterIsAlpha(c) || c == '_') {
			auto start = i;
			while (i < sql.size() && (StringUtil::CharacterIsAlphaNumeric(sql[i]) || sql[i] == '_')) {
				i++;
			}
			if (result.back().size() < word_count) {
				result.back().push_back(StringUtil::Upper(sql.substr(start, i - start)));
			}
		} else {
			i++;
		}
	}
	return result;
}

//! Whether the statements of a query leave the session as it was. Anything unknown counts as a change
static bool LeavesSessionUnchanged(const string &sql) {
	static const case_insensitive_set_t NEUTRAL = {
	    "SELECT", "WITH",   "INSERT",  "UPDATE",   "DELETE", "MERGE",  "COPY",    "BEGIN", "START",
	    "COMMIT", "END",    "ABORT",   "ROLLBACK", "VALUES", "TABLE",  "EXPLAIN", "ALTER", "DROP",
	    "LOCK",   "VACUUM", "ANALYZE", "TRUNCATE", "GRANT",  "REVOKE", "COMMENT", "SAVEPOINT", "RELEASE"};
	auto lower = StringUtil::Lower(sql);
	// functions that change the session from within a statement
	for (auto function : {"set_config", "advisory_lock(", "advisory_lock_shared(", "dblink_connect"}) {
		if (lower.find(function) != string::npos) {
			return false;
		}
	}
	for (auto &words : LeadingWords(sql, 3)) {
		if (words.empty()) {
			continue;
		}
		auto &first = words[0];
		if (first == "SET") {
			// SET LOCAL and SET TRANSACTION end with the transaction
			if (words.size() < 2 || (words[1] != "LOCAL" && words[1] != "TRANSACTION")) {
				return false;
			}
		} else if (first == "CREATE") {
			for (idx_t w = 1; w < words.size(); w++) {
				if (words[w] == "TEMP" || words[w] == "TEMPORARY") {
					return false;
				}
			}
		} else if (NEUTRAL.find(first) == NEUTRAL.end()) {
			return false;
		}
	}
	return true;
}

void PostgresConnection::NoteStatements(const string &sql) {
	if (!session_changed && !LeavesSessionUnchanged(sql)) {
		session_changed = true;
	}
}

void PostgresConnection::Defer(string statement) {
	lock_guard<mutex> guard(connection_lock);
	deferred.push_back(std::move(statement));
}

void PostgresConnection::DeferBegin(string begin) {
	lock_guard<mutex> guard(connection_lock);
	deferred.insert(deferred.begin(), std::move(begin));
	begin_deferred = true;
}

bool PostgresConnection::CancelDeferred() {
	lock_guard<mutex> guard(connection_lock);
	auto result = begin_deferred;
	deferred.clear();
	begin_deferred = false;
	return result;
}

vector<string> PostgresConnection::TakeDeferred() {
	begin_deferred = false;
	auto result = std::move(deferred);
	deferred.clear();
	return result;
}

void PostgresConnection::NoteSessionChanged() {
	session_changed = true;
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
	if (!session_changed) {
		return;
	}
	{
		PGresult *res = PQexec(conn, "DISCARD ALL");
		PostgresResult res_holder(res);
		if (PQresultStatus(res) == PGRES_COMMAND_OK) {
			session_changed = false;
			return;
		}
	}
	PQreset(conn);
	session_changed = false;
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
