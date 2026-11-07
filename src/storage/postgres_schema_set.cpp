#include "storage/postgres_schema_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"

#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/common/shared_ptr.hpp"

#include "postgres_utils.hpp"
#include "storage/postgres_index_set.hpp"
#include "storage/postgres_table_set.hpp"
#include "storage/postgres_type_set.hpp"
#include "storage/postgres_transaction.hpp"
#include "storage/postgres_table_set.hpp"
#include "storage/postgres_catalog.hpp"

namespace duckdb {

PostgresSchemaSet::PostgresSchemaSet(Catalog &catalog, vector<string> schemas_to_load_p)
    : PostgresCatalogSet(catalog, false), schemas_to_load(std::move(schemas_to_load_p)) {
}

vector<unique_ptr<PostgresResultSlice>> SliceResult(PostgresResult &schemas, unique_ptr<PostgresResult> to_slice_ptr) {
	auto shared_result = shared_ptr<PostgresResult>(to_slice_ptr.release());
	auto &to_slice = *shared_result;

	vector<unique_ptr<PostgresResultSlice>> result;
	idx_t current_offset = 0;
	for (idx_t schema_idx = 0; schema_idx < schemas.Count(); schema_idx++) {
		auto oid = schemas.GetInt64(schema_idx, 0);
		idx_t start = current_offset;
		for (; current_offset < to_slice.Count(); current_offset++) {
			auto current_oid = to_slice.GetInt64(current_offset, 0);
			if (current_oid != oid) {
				break;
			}
		}
		result.push_back(make_uniq<PostgresResultSlice>(shared_result, start, current_offset));
	}
	return result;
}

// Variant of SliceResult
// uses field name instead of an integer oid.
// Used by the information_schema introspection path where namespace oids are'nt available.
static vector<unique_ptr<PostgresResultSlice>> SliceResultByName(PostgresResult &schemas,
                                                                 unique_ptr<PostgresResult> to_slice_ptr) {
	auto shared_result = shared_ptr<PostgresResult>(to_slice_ptr.release());
	auto &to_slice = *shared_result;

	vector<unique_ptr<PostgresResultSlice>> result;
	idx_t current_offset = 0;
	for (idx_t schema_idx = 0; schema_idx < schemas.Count(); schema_idx++) {
		auto name = schemas.GetString(schema_idx, 0);
		idx_t start = current_offset;
		for (; current_offset < to_slice.Count(); current_offset++) {
			if (to_slice.GetString(current_offset, 0) != name) {
				break;
			}
		}
		result.push_back(make_uniq<PostgresResultSlice>(shared_result, start, current_offset));
	}
	return result;
}

//! The system schemas are large and rarely used - unless SCHEMA lists them, they are loaded when they are used
static const vector<string> &SystemSchemas() {
	static const vector<string> system_schemas {"information_schema", "pg_catalog", "pg_toast"};
	return system_schemas;
}

string PostgresSchemaSet::GetSchemaCondition(const string &column, const vector<string> &schemas) {
	if (schemas.empty()) {
		return column + " NOT IN (" + PostgresUtils::WriteLiteralsCommaSeparated(SystemSchemas()) + ")";
	}
	return column + " IN (" + PostgresUtils::WriteLiteralsCommaSeparated(schemas) + ")";
}

string PostgresSchemaSet::GetInitializeQuery(const vector<string> &schemas) {
	string base_query = R"(
SELECT oid, nspname
FROM pg_namespace
${CONDITION}
ORDER BY oid;
)";
	auto condition = "WHERE " + GetSchemaCondition("pg_namespace.nspname", schemas);
	return StringUtil::Replace(base_query, "${CONDITION}", condition);
}

string PostgresSchemaSet::GetInitializeQueryInformationSchema(const vector<string> &schemas) {
	string base_query = R"(
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
${CONDITION}
ORDER BY schema_name;
)";
	string condition;
	if (schemas.size() > 0) {
		condition += "AND schema_name IN (" + PostgresUtils::WriteLiteralsCommaSeparated(schemas) + ")";
	}
	return StringUtil::Replace(base_query, "${CONDITION}", condition);
}

void PostgresSchemaSet::LoadEntriesInformationSchema(ClientContext &context, PostgresTransaction &transaction) {
	string schema_query = GetInitializeQueryInformationSchema(schemas_to_load);
	string tables_query = PostgresTableSet::GetInitializeQueryInformationSchema(schemas_to_load);
	// Reading enums, composite types and indexes differs across DBs that emulate PG
	// choice to either return those empty or have vendor specific queries for each.
	// empty for now
	string enum_types_query = "SELECT NULL, NULL, NULL, NULL LIMIT 0;\n";
	string composite_types_query = "SELECT NULL, NULL, NULL, NULL, NULL LIMIT 0;\n";
	string index_query = "SELECT NULL, NULL, NULL LIMIT 0;\n";

	auto full_query = schema_query + tables_query + enum_types_query + composite_types_query + index_query;

	auto results = transaction.ExecuteQueries(context, full_query);
	auto result = std::move(results[0]);
	results.erase(results.begin());
	auto rows = result->Count();

	auto tables = SliceResultByName(*result, std::move(results[0]));
	auto enums = SliceResultByName(*result, std::move(results[1]));
	auto composite_types = SliceResultByName(*result, std::move(results[2]));
	auto indexes = SliceResultByName(*result, std::move(results[3]));
	for (idx_t row = 0; row < rows; row++) {
		auto schema_name = result->GetString(row, 0);
		CreateSchemaInfo info;
		info.SetSchema(Identifier(schema_name));
		info.internal = PostgresSchemaEntry::SchemaIsInternal(schema_name);
		auto schema = make_shared_ptr<PostgresSchemaEntry>(catalog, info, std::move(tables[row]), std::move(enums[row]),
		                                                   std::move(composite_types[row]), std::move(indexes[row]));
		CreateEntry(transaction, std::move(schema));
	}
}

void PostgresSchemaSet::LoadEntries(ClientContext &context, PostgresTransaction &transaction) {
	if (PostgresUtils::UseInformationSchemaIntrospection(context)) {
		LoadEntriesInformationSchema(context, transaction);
		return;
	}
	LoadSchemas(context, transaction, schemas_to_load);
}

optional_ptr<CatalogEntry> PostgresSchemaSet::LoadSchemas(ClientContext &context, PostgresTransaction &transaction,
                                                          const vector<string> &schemas) {
	auto &pg_catalog = catalog.Cast<PostgresCatalog>();
	auto pg_version = pg_catalog.GetPostgresVersion();
	string schema_query = PostgresSchemaSet::GetInitializeQuery(schemas);
	string tables_query = PostgresTableSet::GetInitializeQuery(pg_version, schemas);
	string enum_types_query = PostgresTypeSet::GetInitializeEnumsQuery(pg_version, schemas);
	string composite_types_query = PostgresTypeSet::GetInitializeCompositesQuery(schemas);
	string index_query = PostgresIndexSet::GetInitializeQuery(schemas);

	auto full_query = schema_query + tables_query + enum_types_query + composite_types_query + index_query;

	auto results = transaction.ExecuteQueries(context, full_query);
	auto result = std::move(results[0]);
	results.erase(results.begin());
	auto rows = result->Count();

	auto tables = SliceResult(*result, std::move(results[0]));
	auto enums = SliceResult(*result, std::move(results[1]));
	auto composite_types = SliceResult(*result, std::move(results[2]));
	auto indexes = SliceResult(*result, std::move(results[3]));
	optional_ptr<CatalogEntry> entry;
	for (idx_t row = 0; row < rows; row++) {
		auto oid = result->GetInt64(row, 0);
		auto schema_name = result->GetString(row, 1);
		CreateSchemaInfo info;
		info.SetQualifiedName(
		    QualifiedName(info.GetQualifiedName().Catalog(), Identifier(schema_name), info.GetQualifiedName().Name()));
		info.internal = PostgresSchemaEntry::SchemaIsInternal(schema_name);
		auto schema = make_shared_ptr<PostgresSchemaEntry>(catalog, info, std::move(tables[row]), std::move(enums[row]),
		                                                   std::move(composite_types[row]), std::move(indexes[row]));
		entry = CreateEntry(transaction, std::move(schema));
	}
	return entry;
}

optional_ptr<CatalogEntry> PostgresSchemaSet::ReloadEntry(PostgresTransaction &transaction, const string &name) {
	auto &context = *transaction.GetContext();
	auto &system_schemas = SystemSchemas();
	auto system_schema = std::find_if(system_schemas.begin(), system_schemas.end(),
	                                  [&](const string &schema) { return StringUtil::CIEquals(schema, name); });
	// only the system schemas are left out when the schemas are loaded
	if (!schemas_to_load.empty() || system_schema == system_schemas.end() ||
	    PostgresUtils::UseInformationSchemaIntrospection(context)) {
		return nullptr;
	}
	return LoadSchemas(context, transaction, {*system_schema});
}

optional_ptr<CatalogEntry> PostgresSchemaSet::CreateSchema(PostgresTransaction &transaction, CreateSchemaInfo &info) {
	string create_sql = "CREATE SCHEMA ";
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		create_sql += " IF NOT EXISTS";
	}
	create_sql += PostgresUtils::WriteIdentifier(info.GetQualifiedName().Schema().GetIdentifierName());
	transaction.Query(create_sql);
	auto info_copy = info.Copy();
	info.internal = PostgresSchemaEntry::SchemaIsInternal(info_copy->GetQualifiedName().Schema().GetIdentifierName());
	auto schema_entry = make_shared_ptr<PostgresSchemaEntry>(catalog, info_copy->Cast<CreateSchemaInfo>());
	return CreateEntry(transaction, std::move(schema_entry));
}

} // namespace duckdb
