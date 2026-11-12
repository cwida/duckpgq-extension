// LD_PRELOAD shim (state is heap-allocated and never destroyed, so the exit handler can still read it): times duckdb::StorageLock::GetExclusiveLock / GetSharedLock (both reached through the PLT of the
// frozen libduckdb.so), without changing the library. Both return unique_ptr<StorageLockKey> through a hidden
// pointer: (sret, this). Exclusive acquisitions are logged one per line; shared ones are summed per caller.
#include <dlfcn.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <mutex>
#include <vector>
#include <map>
#include <unistd.h>
#include <sys/syscall.h>
typedef void *(*lock_fn)(void *, void *);
static lock_fn real_x, real_s;
struct Ev { long start_ns, wait_ns; void *caller; long tid; };
static std::mutex &mu = *new std::mutex;
static std::vector<Ev> &evs = *new std::vector<Ev>;
struct Agg { long n, wait_ns, max_ns; };
static std::map<void *, Agg> &shared_agg = *new std::map<void *, Agg>;
static long now_ns() { timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec * 1000000000L + t.tv_nsec; }
static const char *sym(void *a) {
	static std::map<void *, const char *> cache; // dladdr on a 79 MB library is slow: once per caller
	auto it = cache.find(a);
	if (it != cache.end()) return it->second;
	Dl_info i;
	return cache[a] = (dladdr(a, &i) && i.dli_sname ? i.dli_sname : "?");
}
static void dump() {
	const char *path = getenv("LOCKSHIM_OUT");
	FILE *f = fopen(path ? path : "/tmp/lockshim.out", "w");
	if (!f) return;
	std::lock_guard<std::mutex> g(mu);
	fprintf(f, "kind,start_ns,wait_ns,tid,caller\n");
	for (auto &e : evs) fprintf(f, "exclusive,%ld,%ld,%ld,%s\n", e.start_ns, e.wait_ns, e.tid, sym(e.caller));
	for (auto &kv : shared_agg) fprintf(f, "shared_sum,%ld,%ld,%ld,%s\n", kv.second.n, kv.second.wait_ns, kv.second.max_ns, sym(kv.first));
	fclose(f);
}
__attribute__((constructor)) static void init() { atexit(dump); }
extern "C" void *shim_x(void *sret, void *self) asm("_ZN6duckdb11StorageLock16GetExclusiveLockEv");
extern "C" void *shim_x(void *sret, void *self) {
	if (!real_x) real_x = (lock_fn)dlsym(RTLD_NEXT, "_ZN6duckdb11StorageLock16GetExclusiveLockEv");
	void *caller = __builtin_return_address(0);
	long t0 = now_ns();
	void *r = real_x(sret, self);
	long t1 = now_ns();
	std::lock_guard<std::mutex> g(mu);
	evs.push_back({t0, t1 - t0, caller, syscall(SYS_gettid)});
	return r;
}
extern "C" void *shim_s(void *sret, void *self) asm("_ZN6duckdb11StorageLock13GetSharedLockEv");
extern "C" void *shim_s(void *sret, void *self) {
	if (!real_s) real_s = (lock_fn)dlsym(RTLD_NEXT, "_ZN6duckdb11StorageLock13GetSharedLockEv");
	void *caller = __builtin_return_address(0);
	long t0 = now_ns();
	void *r = real_s(sret, self);
	long w = now_ns() - t0;
	std::lock_guard<std::mutex> g(mu);
	auto &a = shared_agg[caller];
	a.n++; a.wait_ns += w; if (w > a.max_ns) a.max_ns = w;
	return r;
}
