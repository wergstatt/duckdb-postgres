#include "duckdb.hpp"

#include "postgres_connection.hpp"
#include "postgres_scanner.hpp"
#include "postgres_utils.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"

namespace duckdb {

struct PostgresExecutePipelineBindData : public TableFunctionData {
	string catalog_name;
	vector<PostgresPipelineStatement> statements;
};

struct PostgresExecutePipelineState : public GlobalTableFunctionState {
	vector<PostgresPipelineResult> results;
	idx_t offset = 0;
};

static vector<PostgresPipelineStatement> ExtractStatements(const Value &statements) {
	vector<PostgresPipelineStatement> result;
	for (auto &statement : ListValue::GetChildren(statements)) {
		if (statement.IsNull() || StructValue::GetChildren(statement)[0].IsNull()) {
			throw BinderException("The statements of postgres_execute_pipeline cannot be NULL");
		}
		auto &fields = StructValue::GetChildren(statement);
		auto params = fields[1].IsNull() ? vector<Value>() : ListValue::GetChildren(fields[1]);
		result.push_back({StringValue::Get(fields[0]), PostgresParameters::Text(std::move(params))});
	}
	return result;
}

static unique_ptr<FunctionData> PostgresExecutePipelineBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<Identifier> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("Parameters to postgres_execute_pipeline cannot be NULL");
	}
	auto result = make_uniq<PostgresExecutePipelineBindData>();
	result->catalog_name = input.inputs[0].GetValue<string>();
	result->statements = ExtractStatements(input.inputs[1]);
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("rowcount");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("result");
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> PostgresExecutePipelineInit(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	auto &data = input.bind_data->Cast<PostgresExecutePipelineBindData>();
	auto &catalog = PostgresUtils::GetCatalogByName(context, data.catalog_name);
	auto &transaction = Transaction::Get(context, catalog).Cast<PostgresTransaction>();
	auto result = make_uniq<PostgresExecutePipelineState>();
	// executed here and not at bind, so that EXPLAIN does not run the statements
	result->results = transaction.GetConnection().ExecutePipeline(context, data.statements);
	return std::move(result);
}

static void PostgresExecutePipelineScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<PostgresExecutePipelineState>();
	auto count = MinValue<idx_t>(state.results.size() - state.offset, STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < count; i++) {
		auto &result = state.results[state.offset++];
		output.SetValue(0, i, Value::BIGINT(result.rows));
		output.SetValue(1, i, result.value);
	}
	output.SetCardinality(count);
}

static LogicalType PipelineStatementsType() {
	child_list_t<LogicalType> statement {{"query", LogicalType::VARCHAR},
	                                     {"params", LogicalType::LIST(LogicalType::VARCHAR)}};
	return LogicalType::LIST(LogicalType::STRUCT(std::move(statement)));
}

PostgresExecutePipelineFunction::PostgresExecutePipelineFunction()
    : TableFunction("postgres_execute_pipeline", {LogicalType::VARCHAR, PipelineStatementsType()},
                    PostgresExecutePipelineScan, PostgresExecutePipelineBind, PostgresExecutePipelineInit) {
}

} // namespace duckdb
