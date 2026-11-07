//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "postgres_connection.hpp"
#include "storage/postgres_connection_pool.hpp"

namespace duckdb {
class PostgresCatalog;
class PostgresCatalogSet;
class PostgresSchemaEntry;
class PostgresTableEntry;

enum class PostgresTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class PostgresTransaction : public Transaction {
public:
	PostgresTransaction(PostgresCatalog &postgres_catalog, TransactionManager &manager, ClientContext &context);
	~PostgresTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	PostgresConnection &GetConnectionWithoutTransaction();
	PostgresConnection &GetConnection();
	//! ID of the pooled connection used by this transaction
	uint64_t GetPoolConnectionId();
	optional_ptr<ClientContext> GetContext();

	string GetDSN();
	unique_ptr<PostgresResult> Query(const string &query);
	unique_ptr<PostgresResult> QueryWithoutTransaction(const string &query);
	vector<unique_ptr<PostgresResult>> ExecuteQueries(ClientContext &context, const string &queries);
	//! Executes statements and the COMMIT of this transaction in one round trip
	vector<PostgresPipelineResult> ExecutePipelineAndCommit(ClientContext &context,
	                                                        const vector<PostgresPipelineStatement> &statements,
	                                                        bool keep_results = false);
	//! Keeps a result of a pipeline until this transaction ends, also after a pipeline committed the transaction on
	//! the server, and returns its id
	idx_t KeepResult(unique_ptr<PostgresResult> result);
	//! The result kept with the id
	shared_ptr<PostgresResult> GetKeptResult(idx_t id);
	static PostgresTransaction &Get(ClientContext &context, Catalog &catalog);
	static string GetBeginTransactionQuery(PostgresIsolationLevel isolation_level, AccessMode access_mode);

	optional_ptr<CatalogEntry> ReferenceEntry(shared_ptr<CatalogEntry> &entry);

	string GetTemporarySchema();

	PostgresTransactionState GetTransactionState() const {
		return transaction_state;
	}
	void StageStalenessSignature(PostgresCatalogSet &catalog_set, string signature);

private:
	PostgresPoolConnection connection;
	PostgresTransactionState transaction_state;
	AccessMode access_mode;
	PostgresIsolationLevel isolation_level;
	bool commit_reads_without_waiting;
	string temporary_schema;
	mutex referenced_entries_lock;
	reference_map_t<CatalogEntry, shared_ptr<CatalogEntry>> referenced_entries;
	mutex pending_signatures_lock;
	vector<pair<reference<PostgresCatalogSet>, string>> pending_signatures;
	mutex kept_results_lock;
	vector<shared_ptr<PostgresResult>> kept_results;

private:
	//! Retrieves the connection **without** starting a transaction if none is active
	PostgresConnection &GetConnectionRaw();

	string GetBeginTransactionQuery();
};

} // namespace duckdb
