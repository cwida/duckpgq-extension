// Concurrent update workload for the path-finding operator (W16, concurrent pass).
//
// One process opens the benchmark database; a writer connection commits batches of knows inserts and deletes
// from knows_update_log while R reader connections run the path-finding query in a loop. Every reader
// iteration is one read transaction that runs the index query and the build-per-query operator over the same
// snapshot, so their results are compared exactly under concurrent writes. Per-event rows go to a CSV.
//
// Throughput mode (--mode throughput, 29 Oct): no writer; C client connections each run single-pair queries
// (one row of the pair table per query, round robin) for --duration-s seconds on one route, so the queries per
// second of the index route against the build-per-query route can be compared from 1 to 32 clients.
//
// Build: see scripts/pathfinding_benchmark.py (build_concurrent_driver) or
//   g++ -O2 -std=c++17 -Iduckdb/src/include scripts/tools/update_concurrent_driver.cpp \
//       -Lbuild/release/src -lduckdb -Wl,-rpath,$PWD/build/release/src -o build/release/update_concurrent_driver
#include "duckdb.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace duckdb;

struct Options {
	std::string database;
	std::string extension;
	std::string output;
	std::string pair_table = "benchmark_pairs_same_src_1024";
	std::string index_name = "pathfinding_benchmark_csr";
	std::string log_table = "knows_update_log";
	idx_t threads = 0;
	idx_t readers = 4;
	idx_t batch_size = 1000;
	idx_t batches = 100;
	idx_t writer_pause_ms = 0;
	//! Workers per path-finding query. The BFS kernels synchronize their tasks with barriers, so concurrent queries
	//! whose tasks together exceed the scheduler's threads deadlock; the wrapper sets threads / readers.
	idx_t threads_per_batch = 0;
	double fold_ratio = 0.1;
	bool run_build = true;
	bool run_index = true;
	idx_t vertex_count = 0;
	std::string catalog = "memory";
	//! "updates" (writer + readers) or "throughput" (clients running single-pair queries for duration_s)
	std::string mode = "updates";
	double duration_s = 20;
};

struct BenchEvent {
	std::string kind; // commit, query_index, query_build, mismatch
	idx_t thread;
	idx_t seq;
	double start_s;
	double elapsed_s;
	std::string reachable;
	std::string total_len;
	std::string edges;
	std::string stats;
};

static std::mutex events_lock;
static std::vector<BenchEvent> events;
static std::atomic<bool> writer_done {false};
static std::atomic<bool> failed {false};
static std::atomic<idx_t> commits_done {0};
static std::chrono::steady_clock::time_point t0;

static double Now() {
	return std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
}

static void Record(BenchEvent event) {
	std::lock_guard<std::mutex> guard(events_lock);
	events.push_back(std::move(event));
}

static std::string Quote(const std::string &value) {
	std::string out = "'";
	for (auto c : value) {
		out += c == '\'' ? "''" : std::string(1, c);
	}
	return out + "'";
}

static unique_ptr<QueryResult> Must(Connection &con, const std::string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw std::runtime_error(result->GetError() + "\nSQL: " + sql);
	}
	return result;
}

static std::string Scalar(QueryResult &result, idx_t col) {
	auto chunk = result.Fetch();
	if (!chunk || chunk->size() == 0) {
		return "";
	}
	auto value = chunk->GetValue(col, 0);
	return value.IsNull() ? "NULL" : value.ToString();
}

static void Setup(Connection &con, const Options &options) {
	Must(con, "LOAD " + Quote(options.extension) + ";");
	if (options.threads > 0) {
		Must(con, "SET threads=" + std::to_string(options.threads) + ";");
	}
	Must(con, "SET experimental_path_finding_operator=true;");
	Must(con, "SET duckpgq_csr_index_fold_ratio = " + std::to_string(options.fold_ratio) + ";");
	if (options.threads_per_batch > 0) {
		Must(con, "SET experimental_path_finding_operator_threads_per_batch=" +
		              std::to_string(options.threads_per_batch) + ";");
	}
}

static std::string IndexKey(const Options &options) {
	return "csr-index|" + options.catalog + "|main|person_knows_person|" + options.index_name;
}

//! pair_filter: "" for the whole pair table, or " WHERE k = <n>" for one pair (throughput mode)
static std::string IndexQuery(const Options &options, const std::string &pair_filter = "") {
	return "SELECT count(len), sum(len) FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, " +
	       Quote(IndexKey(options)) + ") AS len FROM pathfinding_index_pairs" + pair_filter + ");";
}

static std::string BuildQuery(const Options &options, const std::string &edge_count, const std::string &pair_filter = "") {
	return "SELECT count(len), sum(len) FROM (SELECT iterativelengthoperator(src, dst, struct_pack(src := "
	       "pathfinding_edge_src, dst := pathfinding_edge_dst), " +
	       std::to_string(options.vertex_count) + "::BIGINT, " + edge_count +
	       "::BIGINT, 'graph:concurrent') AS len FROM (SELECT src, dst FROM pathfinding_rowid_pairs" + pair_filter +
	       ") pathfinding_pairs"
	       ", (SELECT a.rowid::BIGINT AS pathfinding_edge_src, c.rowid::BIGINT AS pathfinding_edge_dst FROM "
	       "person_knows_person k JOIN person a ON a.id = k.person1id JOIN person c ON c.id = k.person2id) "
	       "pathfinding_edges);";
}

//! Both pair forms with a row number k, in the pair table's order
static void CreatePairTables(Connection &con, const Options &options) {
	Must(con, "CREATE TEMP TABLE pathfinding_rowid_pairs AS SELECT row_number() OVER () - 1 AS k, src, dst FROM " +
	              options.pair_table + ";");
	Must(con, "CREATE TEMP TABLE pathfinding_index_pairs AS SELECT p.k, a.id::BIGINT AS src, b.id::BIGINT AS dst FROM "
	          "pathfinding_rowid_pairs p JOIN person a ON a.rowid = p.src JOIN person b ON b.rowid = p.dst;");
}

//! Throughput client: single-pair queries, round robin over the pair table, until the duration has elapsed
static void ThroughputClientBody(DuckDB &db, const Options &options, idx_t client_id) {
	Connection con(db);
	Setup(con, options);
	CreatePairTables(con, options);
	auto pair_count = std::stoull(Scalar(*Must(con, "SELECT count(*) FROM pathfinding_rowid_pairs;"), 0));
	auto edges = Scalar(*Must(con, "SELECT count(*) FROM person_knows_person;"), 0);
	idx_t seq = 0;
	while (Now() < options.duration_s && !failed.load()) {
		auto k = (seq * options.readers + client_id - 1) % pair_count;
		auto filter = " WHERE k = " + std::to_string(k);
		seq++;
		auto query = options.run_index ? IndexQuery(options, filter) : BuildQuery(options, edges, filter);
		auto start = Now();
		auto t_start = std::chrono::steady_clock::now();
		auto result = Must(con, query);
		auto chunk = result->Fetch();
		auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - t_start).count();
		auto reach = chunk->GetValue(0, 0).ToString();
		auto total = chunk->GetValue(1, 0).IsNull() ? "NULL" : chunk->GetValue(1, 0).ToString();
		Record({options.run_index ? "query_index" : "query_build", client_id, seq, start, elapsed, reach, total, edges,
		        "pair=" + std::to_string(k)});
	}
}

static void ThroughputClient(DuckDB &db, const Options &options, idx_t client_id) {
	try {
		ThroughputClientBody(db, options, client_id);
	} catch (std::exception &ex) {
		std::string what = ex.what();
		for (auto &c : what) {
			if (c == '\n' || c == '"')
				c = ' ';
		}
		Record({"error", client_id, 0, Now(), 0, "", "", "", what});
		failed = true;
	}
}


static void WriterBody(DuckDB &db, const Options &options) {
	Connection con(db);
	Setup(con, options);
	for (idx_t batch = 0; batch < options.batches; batch++) {
		auto lo = batch * options.batch_size;
		auto hi = lo + options.batch_size;
		auto range = " WHERE seq > " + std::to_string(lo) + " AND seq <= " + std::to_string(hi);
		auto start = Now();
		auto t_start = std::chrono::steady_clock::now();
		Must(con, "BEGIN TRANSACTION;");
		Must(con, "INSERT INTO person_knows_person SELECT person1id, person2id FROM " + options.log_table + range +
		              " AND op = 'insert';");
		Must(con, "DELETE FROM person_knows_person k USING (SELECT person1id, person2id FROM " + options.log_table +
		              range + " AND op = 'delete') u WHERE k.person1id = u.person1id AND k.person2id = u.person2id;");
		Must(con, "COMMIT;");
		auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - t_start).count();
		commits_done++;
		Record({"commit", 0, batch + 1, start, elapsed, "", "", "", ""});
		if (options.writer_pause_ms > 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(options.writer_pause_ms));
		}
	}
	writer_done = true;
}

static void Writer(DuckDB &db, const Options &options) {
	try {
		WriterBody(db, options);
	} catch (std::exception &ex) {
		std::string what = ex.what();
		for (auto &c : what) {
			if (c == '\n' || c == '"')
				c = ' ';
		}
		Record({"error", 0, 0, Now(), 0, "", "", "", what});
		failed = true;
		writer_done = true;
	}
}

static void ReaderBody(DuckDB &db, const Options &options, idx_t reader_id) {
	Connection con(db);
	Setup(con, options);
	CreatePairTables(con, options);
	auto index_query = IndexQuery(options);
	idx_t seq = 0;
	while (true) {
		bool last = writer_done.load();
		seq++;
		Must(con, "BEGIN TRANSACTION;");
		auto edges = Scalar(*Must(con, "SELECT count(*) FROM person_knows_person;"), 0);
		auto commits = std::to_string(commits_done.load());
		std::string index_reach, index_total, build_reach, build_total;
		if (options.run_index) {
			auto stats = Scalar(
			    *Must(con, "SELECT duckpgq_csr_index_stats('person_knows_person', " + Quote(options.index_name) + ");"),
			    0);
			auto start = Now();
			auto t_start = std::chrono::steady_clock::now();
			auto result = Must(con, index_query);
			auto chunk = result->Fetch();
			auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - t_start).count();
			index_reach = chunk->GetValue(0, 0).ToString();
			index_total = chunk->GetValue(1, 0).IsNull() ? "NULL" : chunk->GetValue(1, 0).ToString();
			Record({"query_index", reader_id, seq, start, elapsed, index_reach, index_total, edges,
			        stats + ";commits=" + commits});
		}
		if (options.run_build) {
			auto start = Now();
			auto t_start = std::chrono::steady_clock::now();
			auto result = Must(con, BuildQuery(options, edges));
			auto chunk = result->Fetch();
			auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - t_start).count();
			build_reach = chunk->GetValue(0, 0).ToString();
			build_total = chunk->GetValue(1, 0).IsNull() ? "NULL" : chunk->GetValue(1, 0).ToString();
			Record(
			    {"query_build", reader_id, seq, start, elapsed, build_reach, build_total, edges, "commits=" + commits});
		}
		Must(con, "COMMIT;");
		if (options.run_index && options.run_build && (index_reach != build_reach || index_total != build_total)) {
			Record({"mismatch", reader_id, seq, Now(), 0, index_reach + "/" + build_reach,
			        index_total + "/" + build_total, edges, ""});
		}
		if (last || failed.load()) {
			break;
		}
	}
}

static void Reader(DuckDB &db, const Options &options, idx_t reader_id) {
	try {
		ReaderBody(db, options, reader_id);
	} catch (std::exception &ex) {
		std::string what = ex.what();
		for (auto &c : what) {
			if (c == '\n' || c == '"')
				c = ' ';
		}
		Record({"error", reader_id, 0, Now(), 0, "", "", "", what});
		failed = true;
	}
}

int main(int argc, char **argv) {
	Options options;
	for (int i = 1; i < argc; i++) {
		std::string arg = argv[i];
		auto next = [&]() -> std::string {
			if (i + 1 >= argc) {
				throw std::runtime_error("missing value for " + arg);
			}
			return argv[++i];
		};
		if (arg == "--database")
			options.database = next();
		else if (arg == "--extension")
			options.extension = next();
		else if (arg == "--output")
			options.output = next();
		else if (arg == "--pair-table")
			options.pair_table = next();
		else if (arg == "--index-name")
			options.index_name = next();
		else if (arg == "--threads")
			options.threads = std::stoull(next());
		else if (arg == "--readers")
			options.readers = std::stoull(next());
		else if (arg == "--batch-size")
			options.batch_size = std::stoull(next());
		else if (arg == "--batches")
			options.batches = std::stoull(next());
		else if (arg == "--writer-pause-ms")
			options.writer_pause_ms = std::stoull(next());
		else if (arg == "--threads-per-batch")
			options.threads_per_batch = std::stoull(next());
		else if (arg == "--fold-ratio")
			options.fold_ratio = std::stod(next());
		else if (arg == "--no-build")
			options.run_build = false;
		else if (arg == "--no-index")
			options.run_index = false;
		else if (arg == "--mode")
			options.mode = next();
		else if (arg == "--duration-s")
			options.duration_s = std::stod(next());
		else {
			fprintf(stderr, "unknown argument %s\n", arg.c_str());
			return 2;
		}
	}
	if (options.database.empty() || options.extension.empty() || options.output.empty()) {
		fprintf(stderr, "usage: --database <db> --extension <ext> --output <csv> [--readers R --batches N "
		                "--batch-size B --threads T --fold-ratio F --writer-pause-ms P --pair-table name "
		                "--no-build|--no-index --mode updates|throughput --duration-s S]\n");
		return 2;
	}
	try {
		DBConfig config;
		config.SetOptionByName("allow_unsigned_extensions", Value(true));
		if (options.threads > 0) {
			config.SetOptionByName("threads", Value::BIGINT(NumericCast<int64_t>(options.threads)));
		}
		DuckDB db(options.database, &config);
		{
			Connection con(db);
			Setup(con, options);
			options.vertex_count = std::stoull(Scalar(*Must(con, "SELECT count(*) FROM person;"), 0));
			// A file database's catalog is named after the file; the cache key must name it
			options.catalog = Scalar(*Must(con, "SELECT current_database();"), 0);
			Must(con, "CREATE INDEX IF NOT EXISTS " + options.index_name +
			              " ON person_knows_person USING CSR (person1id, person2id);");
			// The extension creates its internal catalog table on first use; two connections doing that at once hit
			// a catalog write-write conflict (seen in this driver), so initialize it once here
			Must(con, "SELECT delete_csr(2147483647);");
			// Resident index before the clock starts
			Must(con, "SELECT duckpgq_csr_index_stats('person_knows_person', " + Quote(options.index_name) + ");");
		}
		t0 = std::chrono::steady_clock::now();
		std::vector<std::thread> threads;
		if (options.mode == "throughput") {
			if (options.run_index == options.run_build) {
				throw std::runtime_error("throughput mode runs one route: pass --no-build or --no-index");
			}
			for (idx_t r = 0; r < options.readers; r++) {
				threads.emplace_back(ThroughputClient, std::ref(db), std::cref(options), r + 1);
			}
		} else {
			threads.emplace_back(Writer, std::ref(db), std::cref(options));
			for (idx_t r = 0; r < options.readers; r++) {
				threads.emplace_back(Reader, std::ref(db), std::cref(options), r + 1);
			}
		}
		for (auto &thread : threads) {
			thread.join();
		}
		std::ofstream out(options.output);
		out << "kind,thread,seq,start_s,elapsed_s,reachable_count,total_len,edges,stats\n";
		for (auto &event : events) {
			out << event.kind << "," << event.thread << "," << event.seq << "," << event.start_s << ","
			    << event.elapsed_s << "," << event.reachable << "," << event.total_len << "," << event.edges << ",\""
			    << event.stats << "\"\n";
		}
		idx_t mismatches = 0;
		idx_t errors = 0;
		for (auto &event : events) {
			mismatches += event.kind == "mismatch";
			if (event.kind == "error") {
				errors++;
				fprintf(stderr, "thread %zu: %s\n", (size_t)event.thread, event.stats.c_str());
			}
		}
		fprintf(stderr, "events=%zu commits=%zu mismatches=%zu errors=%zu wall=%.1fs\n", events.size(),
		        (size_t)commits_done.load(), (size_t)mismatches, (size_t)errors, Now());
		return (mismatches || errors) ? 1 : 0;
	} catch (std::exception &ex) {
		fprintf(stderr, "error: %s\n", ex.what());
		return 1;
	}
}
