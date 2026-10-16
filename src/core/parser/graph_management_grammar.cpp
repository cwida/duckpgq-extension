#include "duckpgq/core/parser/duckpgq_grammar.hpp"
#include "duckpgq/core/parser/duckpgq_parser.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckpgq/parser/parsed_data/drop_property_graph_info.hpp"
#include "duckpgq_graph_management_grammar.hpp"

namespace duckdb {

namespace {

// 'DROP' 'PROPERTY' 'GRAPH' IfExists? QualifiedName
arena_ptr<TransformResultValue> TransformDropGraph(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto if_exists = list.Child<OptionalParseResult>(3).HasResult();
	auto graph_name = transformer.Transform<QualifiedName>(list.GetChild(4));
	auto statement = make_uniq<DropStatement>();
	auto info = make_uniq<DropPropertyGraphInfo>(graph_name.Name().GetIdentifierName(), if_exists);
	info->SetQualifiedName(graph_name);
	statement->info = std::move(info);
	// Planned by the parser extension, which maps it onto the drop_property_graph table function.
	auto parse_data = make_uniq_base<ParserExtensionParseData, DuckPGQParseData>(std::move(statement));
	unique_ptr<SQLStatement> result = make_uniq<ExtensionStatement>(DuckPGQParserExtension(), std::move(parse_data));
	return transformer.MakeResult<unique_ptr<SQLStatement>>(std::move(result));
}

// DuckPGQDescribeKeyword 'PROPERTY' 'GRAPH' QualifiedName
arena_ptr<TransformResultValue> TransformDescribeGraph(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto keyword = DuckPGQLiteralText(list.GetChild(0));
	auto name = transformer.Transform<QualifiedName>(list.GetChild(3)).ToString();
	// Rewrite to a native SELECT over the existing table function: SELECT * FROM <function>('<qualified graph>').
	const char *function_name = keyword == "SUMMARIZE" ? "summarize_property_graph" : "describe_property_graph";
	vector<unique_ptr<ParsedExpression>> arguments;
	arguments.push_back(ConstantExpression::String(name));
	auto function = make_uniq<TableFunctionRef>();
	function->function = make_uniq<FunctionExpression>(Identifier(function_name), std::move(arguments));
	auto node = make_uniq<SelectNode>();
	node->select_list.push_back(make_uniq<StarExpression>());
	node->from_table = std::move(function);
	// Like the native DESCRIBE alternatives, this yields the query node; the enclosing rule builds the statement.
	unique_ptr<QueryNode> result = std::move(node);
	return transformer.MakeResult<unique_ptr<QueryNode>>(std::move(result));
}

arena_ptr<TransformProcess> StartDropGraphTransform(PEGTransformer &transformer, ParseResult &parse_result) {
	return transformer.MakeProcess<FinalizeTransformProcess>(transformer, parse_result, TransformDropGraph);
}

arena_ptr<TransformProcess> StartDescribeGraphTransform(PEGTransformer &transformer, ParseResult &parse_result) {
	return transformer.MakeProcess<FinalizeTransformProcess>(transformer, parse_result, TransformDescribeGraph);
}

} // namespace

void DuckPGQAddGraphManagementGrammar(vector<GrammarChange> &changes) {
	for (auto rule : DUCKPGQ_GRAPH_MANAGEMENT_RULES) {
		changes.push_back(GrammarChange::AddRule(rule));
	}
	changes.push_back(GrammarChange::SetTransformProcess("DuckPGQDropPropertyGraph", StartDropGraphTransform));
	changes.push_back(GrammarChange::SetTransformProcess("DuckPGQDescribePropertyGraph", StartDescribeGraphTransform));
	// Tried before the native DROP statement.
	changes.push_back(GrammarChange::PrependChoice("Statement", "DuckPGQDropPropertyGraph"));
	// Joins the native DESCRIBE/SHOW/SUMMARIZE family, so it also works as a subquery: FROM (DESCRIBE PROPERTY GRAPH
	// g).
	changes.push_back(GrammarChange::PrependChoice("DescribeStatement", "DuckPGQDescribePropertyGraph"));
}

} // namespace duckdb
