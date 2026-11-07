#include "duckdb.hpp"

#include "postgres_binary_parser.hpp"
#include "postgres_connection.hpp"
#include "postgres_scanner.hpp"
#include "postgres_utils.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"

namespace duckdb {

struct PostgresExecutePipelineBindData : public TableFunctionData {
	string catalog_name;
	vector<PostgresPipelineStatement> statements;
	bool commit = false;
	bool keep_results = false;
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
	auto commit = input.named_parameters.find(Identifier("commit"));
	if (commit != input.named_parameters.end() && !commit->second.IsNull()) {
		result->commit = BooleanValue::Get(commit->second);
	}
	auto keep_results = input.named_parameters.find(Identifier("keep_results"));
	if (keep_results != input.named_parameters.end() && !keep_results->second.IsNull()) {
		result->keep_results = BooleanValue::Get(keep_results->second);
	}
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("rowcount");
	// a kept result is read with postgres_pipeline_result, by its id in place of its first value
	return_types.emplace_back(result->keep_results ? LogicalType::BIGINT : LogicalType::VARCHAR);
	names.emplace_back(result->keep_results ? "result_id" : "result");
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> PostgresExecutePipelineInit(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	auto &data = input.bind_data->Cast<PostgresExecutePipelineBindData>();
	auto &catalog = PostgresUtils::GetCatalogByName(context, data.catalog_name);
	auto &transaction = Transaction::Get(context, catalog).Cast<PostgresTransaction>();
	auto result = make_uniq<PostgresExecutePipelineState>();
	// executed here and not at bind, so that EXPLAIN does not run the statements
	if (data.commit) {
		result->results = transaction.ExecutePipelineAndCommit(context, data.statements, data.keep_results);
	} else {
		result->results =
		    transaction.GetConnection().ExecutePipeline(context, data.statements, false, data.keep_results);
	}
	if (data.keep_results) {
		for (auto &statement_result : result->results) {
			// the id of the kept result, NULL for a statement that returns no columns
			auto &kept = statement_result.result;
			statement_result.value = kept ? Value::BIGINT(NumericCast<int64_t>(transaction.KeepResult(std::move(kept))))
			                              : Value(LogicalType::BIGINT);
		}
	}
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

static FunctionSignature PostgresExecutePipelineSignature() {
	FunctionSignature signature;
	signature.AddParameter("database", LogicalType::VARCHAR)
	    .AddParameter("statements", PipelineStatementsType())
	    .WithTypedKwargs("options", [&](TypedKwargs &options) {
		    options.Add("commit", LogicalType::BOOLEAN).Add("keep_results", LogicalType::BOOLEAN);
	    });
	return signature;
}

PostgresExecutePipelineFunction::PostgresExecutePipelineFunction()
    : TableFunction("postgres_execute_pipeline", PostgresExecutePipelineSignature(), PostgresExecutePipelineScan,
                    PostgresExecutePipelineBind, PostgresExecutePipelineInit) {
}

struct PostgresPipelineResultBindData : public TableFunctionData {
	shared_ptr<PostgresResult> result;
	vector<LogicalType> types;
	vector<PostgresType> postgres_types;
	PostgresTypeConfig type_config;
};

struct PostgresPipelineResultState : public GlobalTableFunctionState {
	idx_t row = 0;
};

static unique_ptr<FunctionData> PostgresPipelineResultBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types,
                                                           vector<Identifier> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("Parameters to postgres_pipeline_result cannot be NULL");
	}
	auto &catalog = PostgresUtils::GetCatalogByName(context, input.inputs[0].GetValue<string>());
	auto &transaction = Transaction::Get(context, catalog).Cast<PostgresTransaction>();
	auto result = make_uniq<PostgresPipelineResultBindData>();
	result->result = transaction.GetKeptResult(input.inputs[1].GetValue<idx_t>());
	result->type_config = PostgresTypeConfig::FromContext(context);
	auto res = result->result->res;
	for (int c = 0; c < PQnfields(res); c++) {
		PostgresType postgres_type;
		postgres_type.oid = PQftype(res, c);
		PostgresTypeData type_data;
		type_data.type_name = PostgresUtils::PostgresOidToName(postgres_type.oid);
		type_data.type_modifier = PQfmod(res, c);
		return_types.push_back(
		    PostgresUtils::TypeToLogicalType(nullptr, nullptr, result->type_config, type_data, postgres_type));
		names.emplace_back(PQfname(res, c));
		if (postgres_type.info == PostgresTypeAnnotation::CAST_TO_VARCHAR) {
			// postgres_query casts such a column in the query it sends, a kept result was sent already
			throw BinderException("Column \"%s\" of a pipeline result has a type that cannot be read in the binary "
			                      "format, cast it to TEXT in the query",
			                      PQfname(res, c));
		}
		result->postgres_types.push_back(postgres_type);
	}
	PostgresUtils::DeduplicateColumns(names);
	result->types = return_types;
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> PostgresPipelineResultInit(ClientContext &context,
                                                                       TableFunctionInitInput &input) {
	return make_uniq<PostgresPipelineResultState>();
}

static void PostgresPipelineResultScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->Cast<PostgresPipelineResultBindData>();
	auto &state = input.global_state->Cast<PostgresPipelineResultState>();
	auto &result = *data.result;
	PostgresBinaryParser parser(data.types, data.postgres_types, data.type_config);
	idx_t count = 0;
	for (; count < STANDARD_VECTOR_SIZE && state.row < result.Count(); count++, state.row++) {
		for (idx_t c = 0; c < output.ColumnCount(); c++) {
			if (result.IsNull(state.row, c)) {
				FlatVector::SetNull(output.data[c], count, true);
				continue;
			}
			auto value = result.GetStringRef(state.row, c);
			parser.SetBuffer(data_ptr_cast(value.GetDataWriteable()), value.GetSize());
			parser.ParseValue(data.types[c], data.postgres_types[c], NumericCast<int32_t>(value.GetSize()),
			                  output.data[c], count);
		}
	}
	output.SetChildCardinality(count);
}

PostgresPipelineResultFunction::PostgresPipelineResultFunction()
    : TableFunction("postgres_pipeline_result", {LogicalType::VARCHAR, LogicalType::BIGINT}, PostgresPipelineResultScan,
                    PostgresPipelineResultBind, PostgresPipelineResultInit) {
}

} // namespace duckdb
