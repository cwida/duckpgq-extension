#include "duckpgq/core/parser/duckpgq_grammar.hpp"
#include "duckpgq/core/parser/duckpgq_parser.hpp"

#include "duckdb/parser/grammar_extension.hpp"

namespace duckdb {

namespace {

class DuckPGQGrammarExtension final : public GrammarExtension {
public:
	DuckPGQGrammarExtension() : GrammarExtension("duckpgq", "SQL/PGQ property graph syntax") {
	}

	vector<GrammarChange> GetChanges() const override {
		vector<GrammarChange> changes;
		DuckPGQAddCreatePropertyGraphGrammar(changes);
		DuckPGQAddGraphTableGrammar(changes);
		DuckPGQAddGraphManagementGrammar(changes);
		return changes;
	}
};

} // namespace

void CorePGQParser::RegisterPGQGrammarExtension(ExtensionLoader &loader) {
	GrammarExtension::Register(loader.GetDatabaseInstance(), make_shared_ptr<DuckPGQGrammarExtension>());
}

} // namespace duckdb
