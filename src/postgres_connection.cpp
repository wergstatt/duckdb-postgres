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
	ReadPendingCommit();
	NoteStatements(query);

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
	auto sent_query = query;
	if (params.Empty() && format == 0) {
		// a plain query can hold several statements, so the deferred ones go with it and the error names them too
		sent_query = PrependDeferred(query);
	} else {
		ExecuteDeferred(context);
	}
	auto result = PQExecute(context, sent_query, params, format);

	if (ResultHasError(result)) {
		if (error_message) {
			auto error = result ? PQresultErrorMessage(result) : PQerrorMessage(GetConn());
			*error_message = StringUtil::Format("Failed to execute query \"" + sent_query + "\": " + string(error));
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

	int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                       .time_since_epoch()
	                       .count();
	if (context) {
		DUCKDB_LOG(*context, PostgresQueryLogType, query, end_time - start_time);
	}

	if (ResultHasError(result)) {
		auto error = result ? PQresultErrorMessage(result) : PQerrorMessage(conn);
		auto error_message = StringUtil::Format("Failed to execute prepared statement \"%s\" for query \"%s\": %s",
		                                        stmt.GetName(), query, string(error));
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
				    "Failed to execute query \"" + sent_queries + "\": " + string(PQresultErrorMessage(res));
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
	DUCKDB_LOG(context, PostgresQueryLogType, sent_queries, end_time - start_time);
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
	return PQresultStatus(res) == PGRES_TUPLES_OK;
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
