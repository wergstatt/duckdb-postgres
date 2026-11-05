#include "postgres_connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "postgres_binary_reader.hpp"

namespace duckdb {

void PostgresConnection::BeginCopyFrom(ClientContext &context, const string &query, ExecStatusType expected_result) {
	lock_guard<mutex> guard(connection_lock);
	auto sent_query = PrependDeferred(query);
	PostgresResult pg_res(PQExecute(context, sent_query));
	auto result = pg_res.res;
	if (!result || PQresultStatus(result) != expected_result) {
		auto error = result ? PQresultErrorMessage(result) : PQerrorMessage(GetConn());
		throw std::runtime_error("Failed to prepare COPY \"" + sent_query + "\": " + string(error));
	}
}

} // namespace duckdb
