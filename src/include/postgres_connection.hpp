//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_connection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "postgres_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "postgres_parameters.hpp"
#include "postgres_result.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {
class PostgresBinaryWriter;
class PostgresTextWriter;
struct PostgresBinaryReader;
class PostgresSchemaEntry;
class PostgresTableEntry;
class PostgresStatement;
class PostgresResult;
class PostgresPreparedStatement;
struct IndexInfo;

//! A statement of a pipeline, with the parameters it refers to as $1, $2, ...
struct PostgresPipelineStatement {
	string query;
	PostgresParameters params;
};

//! The result of a statement of a pipeline
struct PostgresPipelineResult {
	int64_t rows;
	//! The first value of the first row, as text, for statements that return rows
	Value value;
	//! With keep_results, the rows in the binary format instead of the first value, for statements that return columns
	unique_ptr<PostgresResult> result;
};

class PostgresConnection {
public:
	explicit PostgresConnection(PGconn *connection = nullptr);
	~PostgresConnection();
	// disable copy constructors
	PostgresConnection(const PostgresConnection &other) = delete;
	PostgresConnection &operator=(const PostgresConnection &) = delete;
	//! enable move constructors
	PostgresConnection(PostgresConnection &&other) noexcept;
	PostgresConnection &operator=(PostgresConnection &&) noexcept;

public:
	static PostgresConnection Open(const string &dsn, const string &attach_path);
	void Execute(optional_ptr<ClientContext> context, const string &query,
	             const PostgresParameters &params = PostgresParameters(), int format = 0);
	unique_ptr<PostgresResult> TryQuery(optional_ptr<ClientContext> context, const string &query,
	                                    optional_ptr<string> error_message = nullptr,
	                                    const PostgresParameters &params = PostgresParameters(), int format = 0);
	unique_ptr<PostgresResult> Query(optional_ptr<ClientContext> context, const string &query,
	                                 const PostgresParameters &params = PostgresParameters(), int format = 0);
	//! Executes a statement that was prepared on this connection
	unique_ptr<PostgresResult> QueryStmt(optional_ptr<ClientContext> context, const PostgresPreparedStatement &stmt,
	                                     const PostgresParameters &params = PostgresParameters(), int format = 0);

	//! Submits a set of queries to be executed in the connection.
	vector<unique_ptr<PostgresResult>> ExecuteQueries(ClientContext &context, const string &queries);
	//! Executes statements in one round trip and returns the result of each. A failed statement skips the statements
	//! after it, and its error names its position and SQLSTATE. With commit, a COMMIT follows them in the round trip
	vector<PostgresPipelineResult> ExecutePipeline(ClientContext &context,
	                                               const vector<PostgresPipelineStatement> &statements,
	                                               bool commit = false, bool keep_results = false);
	//! Runs queries in one round trip, each on its own as TryQuery runs it: the result of each, nullptr if it failed
	vector<unique_ptr<PostgresResult>> TryQueries(optional_ptr<ClientContext> context, const vector<string> &queries);

	PostgresVersion GetPostgresVersion(ClientContext &context);
	static string GetPostgresVersionQuery();
	//! The version from the result of GetPostgresVersionQuery
	static PostgresVersion GetPostgresVersion(optional_ptr<PostgresResult> result);

	vector<IndexInfo> GetIndexInfo(const string &table_name);

	void BeginCopyTo(ClientContext &context, PostgresCopyState &state, PostgresCopyFormat format,
	                 const string &schema_name, const string &table_name, const vector<string> &column_names);
	void CopyData(data_ptr_t buffer, idx_t size);
	void CopyData(PostgresBinaryWriter &writer);
	void CopyData(PostgresTextWriter &writer);
	void CopyChunk(ClientContext &context, PostgresCopyState &state, DataChunk &chunk, DataChunk &varchar_chunk);
	void FinishCopyTo(PostgresCopyState &state);

	void BeginCopyFrom(ClientContext &context, const string &query, ExecStatusType expected_result);

	bool IsOpen();
	void Close();
	//! Sends a statement with the next request on this connection, instead of in a round trip of its own
	void Defer(string statement);
	//! Sends BEGIN with the next request, so that a transaction that sends nothing does not reach the server
	void DeferBegin(string begin);
	//! Drops a deferred BEGIN that no other statement follows, returns whether it did
	bool CancelBegin();
	//! Commits the transaction. With reads_without_waiting, a transaction that only read sends COMMIT without waiting
	//! for the reply: the server commits at once, and the next request on the connection reads the reply
	void Commit(optional_ptr<ClientContext> context, bool reads_without_waiting);
	//! Sends the deferred statements in a round trip of their own, ahead of a request that cannot carry them. The
	//! connection lock must be held
	void ExecuteDeferred(optional_ptr<ClientContext> context);
	//! Whether the server answered on this connection within the last milliseconds
	bool AnsweredWithin(uint64_t milliseconds) const;
	bool PingServer(const std::string &health_check_query);
	void Reset(const std::string &health_check_query);
	//! Notes that a named statement was prepared on this connection, or closed again
	void NoteStatementPrepared();
	void NoteStatementClosed();

	string GetDSN() {
		return dsn;
	}

	//! Locks the connection, must be held when calling libpq functions on the result of GetConn()
	unique_lock<mutex> Lock() {
		return unique_lock<mutex>(connection_lock);
	}
	PGconn *GetConn() {
		if (!connection) {
			throw InternalException("PostgresConnection::GetConn - no connection available");
		}
		return connection;
	}

	static void DebugSetPrintQueries(bool print);
	static bool DebugPrintQueries();

private:
	//! Requires connection_lock to be held
	PGresult *PQExecute(optional_ptr<ClientContext> context, const string &query,
	                    const PostgresParameters &params = PostgresParameters(), int format = 0);
	//! Requires connection_lock to be held
	bool PingServerInternal(const std::string &health_check_query);
	//! Returns the query with the deferred statements ahead of it, to send them in the same plain query. Requires
	//! connection_lock to be held
	string PrependDeferred(const string &query);
	//! Takes the deferred statements, to send them ahead of a request that holds one statement. Requires
	//! connection_lock to be held
	vector<string> TakeDeferred();
	//! Notes the statements sent on this connection, so that a reset can tell whether they changed the session
	void NoteStatements(const string &sql);
	//! Reads the reply to a COMMIT sent without waiting, if any, and reconnects if it shows the connection lost.
	//! Requires connection_lock to be held
	void ReadPendingCommit();

	//! Notes that the server answered
	void MarkAnswered();

	PGconn *connection = nullptr;
	mutex connection_lock;
	//! The statements to send with the next request
	vector<string> deferred;
	//! Whether the BEGIN of a transaction is among the deferred statements
	bool begin_deferred = false;
	std::chrono::steady_clock::time_point last_answer;
	string dsn;
	//! Whether a statement may have changed the session beyond its transaction, such as a SET or a temporary table
	bool session_changed = false;
	//! The named statements prepared on this connection and not closed yet
	idx_t open_statements = 0;
	//! Whether the statements sent since the last BEGIN only read
	bool only_reads = true;
	//! Whether the reply to a COMMIT sent without waiting is still to be read
	bool commit_pending = false;
};

} // namespace duckdb
