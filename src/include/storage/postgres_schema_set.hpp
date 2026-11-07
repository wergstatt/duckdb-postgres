//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_schema_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/postgres_catalog_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/postgres_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

class PostgresSchemaSet : public PostgresCatalogSet {
public:
	explicit PostgresSchemaSet(Catalog &catalog, vector<string> schemas_to_load);

public:
	optional_ptr<CatalogEntry> CreateSchema(PostgresTransaction &transaction, CreateSchemaInfo &info);

	static string GetInitializeQuery(const vector<string> &schemas);
	static string GetInitializeQueryInformationSchema(const vector<string> &schemas);
	//! The condition on a schema name column that selects the schemas to load - all schemas but the system schemas
	//! if empty, these are loaded when they are used
	static string GetSchemaCondition(const string &column, const vector<string> &schemas);

	optional_ptr<CatalogEntry> ReloadEntry(PostgresTransaction &transaction, const string &name) override;

protected:
	void LoadEntries(ClientContext &context, PostgresTransaction &transaction) override;
	void LoadEntriesInformationSchema(ClientContext &context, PostgresTransaction &transaction);
	optional_ptr<CatalogEntry> LoadSchemas(ClientContext &context, PostgresTransaction &transaction,
	                                       const vector<string> &schemas);
	bool SupportReload() const override {
		return true;
	}

protected:
	//! Schema to load - if empty loads all schemas (default behavior)
	vector<string> schemas_to_load;
};

} // namespace duckdb
