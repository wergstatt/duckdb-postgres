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
	//! after it
	vector<PostgresPipelineResult> ExecutePipeline(ClientContext &context,
	                                               const vector<PostgresPipelineStatement> &statements);

	PostgresVersion GetPostgresVersion(ClientContext &context);

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
	//! Whether the server answered on this connection within the last milliseconds
	bool AnsweredWithin(uint64_t milliseconds) const;
	//! Notes that the session of this connection may have changed, so that a reset has to discard it
	void NoteSessionChanged();
	//! Sends a statement ahead of the next request on this connection, instead of in a round trip of its own
	void Defer(string statement);
	//! Sends BEGIN ahead of the next request
	void DeferBegin(string begin);
	//! Drops the statements that were not sent yet, returns whether that includes the BEGIN of the transaction
	bool CancelDeferred();
	//! Takes the statements to send ahead of the next request, the connection lock must be held
	vector<string> TakeDeferred();
	bool PingServer(const std::string &health_check_query);
	void Reset(const std::string &health_check_query);

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

	//! Notes that the server answered
	void MarkAnswered();
	//! Notes the statements sent on this connection, so a reset can tell whether they changed the session
	void NoteStatements(const string &sql);

	PGconn *connection = nullptr;
	mutex connection_lock;
	std::chrono::steady_clock::time_point last_answer;
	//! Whether a statement may have changed the session, such as a SET or a temporary table
	bool session_changed = false;
	//! The statements to send ahead of the next request
	vector<string> deferred;
	//! Whether the deferred statements include the BEGIN of the transaction
	bool begin_deferred = false;
	string dsn;
};

} // namespace duckdb
