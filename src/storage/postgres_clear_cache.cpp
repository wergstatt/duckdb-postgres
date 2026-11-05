#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "postgres_scanner.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/postgres_catalog.hpp"

namespace duckdb {

struct ClearCacheFunctionData : public TableFunctionData {
	bool finished = false;
	//! The catalog whose cache is cleared, all Postgres catalogs if empty
	string catalog_name;
};

static unique_ptr<FunctionData> ClearCacheBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<ClearCacheFunctionData>();
	auto catalog = input.named_parameters.find(Identifier("catalog"));
	if (catalog != input.named_parameters.end() && !catalog->second.IsNull()) {
		result->catalog_name = StringValue::Get(catalog->second);
	}
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

void PostgresClearCacheFunction::ClearPostgresCaches(ClientContext &context) {
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &db = *db_ref;
		auto &catalog = db.GetCatalog();
		if (catalog.GetCatalogType() != "postgres") {
			continue;
		}
		catalog.Cast<PostgresCatalog>().ClearCache();
	}
}

static void ClearCacheFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<ClearCacheFunctionData>();
	if (data.finished) {
		return;
	}
	if (data.catalog_name.empty()) {
		PostgresClearCacheFunction::ClearPostgresCaches(context);
	} else {
		auto attached_catalog = PostgresCatalog::Lookup(context, Identifier(data.catalog_name));
		attached_catalog.Get<PostgresCatalog>().ClearCache();
	}
	data.finished = true;
}

void PostgresClearCacheFunction::ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter) {
	PostgresClearCacheFunction::ClearPostgresCaches(context);
}

static FunctionSignature ClearCacheSignature() {
	FunctionSignature signature;
	signature.WithTypedKwargs("options", [&](TypedKwargs &options) { options.Add("catalog", LogicalType::VARCHAR); });
	return signature;
}

PostgresClearCacheFunction::PostgresClearCacheFunction()
    : TableFunction("postgres_clear_cache", ClearCacheSignature(), ClearCacheFunction, ClearCacheBind) {
}
} // namespace duckdb
