# Writing style for this paper

Modelled on Peter Boncz's first-author papers (MonetDB/X100, CIDR 2005; FSST, PVLDB 2020) and the papers he
co-wrote closely (Teseo, PVLDB 2021; DuckPGQ, CIDR 2023; Kersten et al., PVLDB 2018). Observations from the texts,
then the rules we follow.

## What his prose does

**Opens with a plain fact about the world, then the consequence.** "Strings are prevalent in real-world data sets.
They often occupy a large fraction of the data and are slow to process." "Modern CPUs can perform enormous amounts
of calculations per second, but only if they can find enough independent work." No throat-clearing, no "In recent
years". The first sentence of a section is a claim the reader can check.

**Argues in the first person plural and says so.** "We argue that", "We question whether it should really be that
way", "We believe these features will make FSST a valuable piece in the standard compression toolbox", "The main
conclusion we draw from this investigation is". Opinions are marked as ours and stated once, firmly.

**States the result chain explicitly.** Premise, consequence, design decision: "However, its policy of full column
materialization causes it to generate large data streams ... Therefore, we argue for combining the column-wise
execution of MonetDB with the incremental materialization offered by Volcano-style pipelining." Connectives carry
the argument: "Therefore", "However", "In other words", "It turns out that", "This is because".

**Quantifies everything that can be quantified, inline.** "(≈ 1-3 cycles/byte, or 1-3 GB/s per core)", "more than
1 microsecond for strings with an average length of 19 ... roughly 100 CPU cycles per character or tens of megabytes
per second, which is too slow". A number is followed by what it means.

**Compares against the strongest alternative by name.** "In comparison with LZ4, which is so far the best
general-purpose lightweight compression method ..., FSST is better over all dimensions." Related work is evaluative:
each approach gets one sentence on what it does and one on why it falls short for this problem.

**Uses parenthetical examples and cross-references freely.** "(e.g., URLs, email addresses, IP addresses, UUIDs,
non-integer surrogate keys)", "(cf. Section 6.1)". Lists of concrete instances replace abstract nouns.

**Puts the key idea in Figure 1 with a long caption** that names the challenge and points to the sections that
solve it. The introduction ends with a bulleted list of key features or numbered contributions, each one line.

**Writes as a system builder.** Mentions the open-source release, the licence, the replication package, how the
technique plugs into existing systems ("can be integrated into existing data management systems and columnar file
formats like Parquet, and should be used in conjunction with dictionary compression"). Footnotes hold asides.

**Keeps sentences medium length and plain.** Near-colloquial verbs and images are allowed when they are exact:
"catch-all type", "strike a balance", "brand new", "a valuable piece in the toolbox", "all technology that makes up
a state-of-the-art relational system". Hedges are rare and specific ("may explain why they have not been widely
adopted"), never ritual.

**Conclusion restates the argument in three steps and names future work concretely.** Problem, finding, design,
numbers; then "In the future, we will" with real items.

## Rules for this paper

1. Start every section and most paragraphs with a checkable claim, not with context.
2. First person plural for everything we did or think. No passive voice for our own work.
3. Every design decision gets its reason in the same paragraph, introduced by "because", "therefore" or "in other
   words".
4. Numbers inline, with their meaning: "0.03 s, one third of the time Kuzu needs for the same query".
5. Name the alternative we beat or lose to. "Native graph systems" is not a name; Kuzu and Neo4j are.
6. Related work: one sentence what, one sentence why it does not solve our problem. Group by problem, not by year.
7. Figure 1 is the architecture (MATCH, operator, index, snapshot) with a caption that points to the sections.
8. Introduction ends with the contributions as a numbered list, one line each, with the headline number.
9. Say what is open source and where, and what the engine would need to make the extension simpler.
10. Em dashes are not used; parentheses and commas carry asides. Short footnotes are fine.

## Terminology

| use | not | why |
|---|---|---|
| traversal (multi-source traversal, single-source traversal, frontier traversal) | kernel | "kernel" reads as GPU or OS jargon; the author's preference |
| path-finding operator | PathFinding operator, BFS operator | matches SQL/PGQ wording |
| CSR index, index snapshot, epoch, change log, fold | cache, persistence | the index is a derived structure under MVCC, not a cache |
| on-the-fly CSR, built inside the query ("build per query" in tables and figure legends) | transient CSR | the CIDR paper's term; the short form fits a table row |
| lanes (of the multi-source traversal) | bitset columns | MS-BFS terminology from Then et al. |
| destination partition, source bucket | shard, chunk | the layout's own names |
| host engine, extension | fork, plugin | the thesis is about the boundary |
| source and destination; (src, dst) row ids | endpoint(s) | the author's preference; say which vertex is meant |
| microbenchmark | ablation | the author's term; say what was switched off or varied |
| vertex key, edge row id | rowid, id | keys identify vertices, row ids identify edges |

## Lessons from the author's proofreading (3 and 4 Nov, about ninety notes on Sections 3 and 4)

The same faults recurred, in this order of frequency. Check a section for them before it is shown.

1. **Terms used before they are defined.** The text grew with the implementation, so it explained things in the
   order they were built. Before writing or restructuring a section, list its terms and the line where each is first
   used and first defined (a script over `\emph{...}` and the vocabulary tables does it); introduce each term where it
   is first needed, with a one-clause definition, and never rely on a later subsection.
2. **A mechanism without its reason.** Every mechanism gets its reason in the same sentence or the next, as a
   measured fact where one exists. If the reason cannot be stated, say so instead of describing around it; twice
   (the row-bucket size, the scheduler's own-threads switch) that was a design smell and the code was simplified.
3. **Pronouns and generic verbs where a vocabulary noun belongs.** No "it", "its view", "reaches", "arrives",
   "gets", "may see" when the referent is more than one sentence back or the meaning has a name in the tables.
   Repeating the noun is right.
4. **Numbers in the wrong place.** A number belongs where the claim it supports is made: the evaluation, or one
   trade-off paragraph. Not inside the description of a mechanism.
5. **Short sentences as pivots.** "The first is visibility." "There is one exception." They read as choppy, not
   emphatic. Fold them into the sentence they introduce.
6. **Implementation detail at the level of overview.** Keep a detail only if a reviewer needs it to believe the
   claim; otherwise it moves down a level or out (dictionary generations, the tombstone lookup path, exact sizes).
7. **Set-up before the point.** A paragraph that lists alternatives and knocks them down before stating its point
   reads as generated. Lead with the point, then give the evidence.

Process lesson: sections patched while the design changed (3 before rc7, 4 before the restructure) drew structural
notes; sections written in one pass from settled code drew wording notes only. Write Sections 5 and 6 once, from the
final code and numbers, rather than patching the current text.

## Vocabulary of Section 3 (decided 29 Oct)

One name per thing, used the same way in the text, the figures and the captions.

| term | meaning | not |
|---|---|---|
| partition | the edges into one destination range, also while the build collects them | bucket, range (for the edges) |
| destination range | the vertex ids a partition covers, at most 65,535 | |
| sparse row | one source with an edge into the partition: its id and the offset of its edges | |
| source index | the index over a partition's sparse rows by the high bits of the source id | bucket index |
| search | one breadth-first search from one distinct source (pairs with the same source share it); it ends at its last destination, at the depth bound or when nothing is left | group, per-source search, frontier search |
| single-source traversal | searches run one per source, each with its own frontier | frontier traversal, per-source kernel |
| multi-source traversal | searches run as lanes of batches | MS-BFS kernel |
| lane | one search inside a batch: one source with its destination list | |
| batch | up to 512 lanes traversed together over one working set (renamed from wave, 2 Nov) | wave |
| working set | a batch's three bit sets per vertex plus the summary bitmap | |
| batches in progress | the batches the scheduler runs at once, each with its working set (replaces "worker group", 2 Nov) | worker group, group |
| level | one breadth-first step; in the multi-source traversal an explore phase and a check phase | iteration |
| distance | the level at which a search first reached a vertex | level stamp |
| sparse level, dense level | a level that visits only the frontier, or one that sweeps every sparse row of every partition | (the 1/64 and 1/16 thresholds are details of each) |
| lengths, paths, reach | the three output modes | |
| build route, index route | the CSR built on the fly in every query, or read from a persistent CSR index | inside the query |

## Vocabulary of Section 4 (decided 3 Nov)

| term | meaning | not |
|---|---|---|
| CSR index | the index created with `CREATE INDEX ... USING CSR` | graph index, cache |
| epoch | an immutable partitioned CSR with its dictionary and one edge row id per edge, built by one transaction | version, snapshot |
| dictionary | the map from vertex keys to the dense ids of one epoch | |
| change log, log entry | the inserts and deletes committed after the newest epoch; one entry per inserted or deleted row | delta (in text) |
| change group | the entries of one `Append` or `Delete` call; visibility is decided once per group | commit group |
| stamp | the mark of the deleting group on the log entry of a deleted row | |
| tombstone | the entry for a deleted row that an epoch contains | |
| snapshot | what one transaction traverses: an epoch plus its overlay | |
| overlay | the suppressed epoch positions and the inserted edges, in the epoch's partitions | delta CSR, companion |
| fold | building a new epoch from the table | compaction (that is the log's cleanup) |
| generation, accounts for | an epoch's number in installation order; an epoch accounts for an entry it already contains or excludes | covered, covered_epoch |
| retained epoch | an older epoch kept for transactions that started before a fold | |
| private epoch | an epoch built for one transaction from its own view (uncommitted changes, or older than every retained epoch) | private CSR |
| transaction | the reader of the index | reader |

## Out of scope for the text

Bugs we fixed along the way (the barrier deadlock under concurrent queries, the NULL-pair fallback, the stale
dictionary ids) are implementation details. Describe the resulting design and its property; do not narrate the bug.

## Captions

A caption states what is plotted, not what we conclude from it, in this order: what the query is, which data sets,
what the axes are with their units and scales, what each line or row is, what a marker means, and how many runs
the number averages. Our own words for the experiment (shape, point, route, budget) do not appear; say "random
pairs", "65 536 pairs", "over the index", "600 s limit". One sentence of interpretation at the end is allowed when
the figure cannot be misread without it.

## Shapes that read as generated

Not to disguise machine writing; these shapes are weaker prose. They perform emphasis instead of stating the fact,
and a reader who has seen them elsewhere stops trusting the text. Pass over every section for them before it is shown.

**Sentence shapes**
- Negative parallelism: "not X but Y", "not just X, it's Y", "X rather than Y" as a reveal. Say what it is.
- Tricolon: three parallel items or three semicolon-joined clauses of equal length. Two items, a plain list, or
  separate sentences.
- Colon reveal: a clause, a colon, then the punchy explanation. Make it a sentence with "because" or a full stop.
- Aphoristic one-liners as paragraph pivots. Fold them into the sentence they support.
- Anaphora and mirrored pairs: "right when X ... right when Y".
- Uniform rhythm: every sentence "X does Y, so Z." Vary length and joiner.
- Rhetorical question then answer; "here's the thing"; "think of it as".

**Words**
- Inflation: pivotal, crucial, robust, seamless, delve, leverage, landscape, tapestry, underscore, showcase,
  intricate, nuanced, comprehensive.
- Copula dodging: "serves as", "functions as", "marks", "stands as" for "is".
- Empty transitions: "it is worth noting", "importantly", "notably", "in conclusion", "overall".
- Personified machinery as a habit: the planner "knows", the scan "beats". One is fine; a page of it is a tic.
  Prefer the plain verb: uses, assigns, reduces, is faster than.
- Vague attribution: "observers note", "research shows".
- Magic adverbs: quietly, deeply, fundamentally, truly.

**Structure**
- Fractal summaries: the point restated at intro, section and end.
- Signposted conclusions, "despite challenges" formulas, bold-first bullets, em-dash asides, heading-only sections,
  section-ending recaps.
- Promotional significance: "marking a shift", "paving the way".

**What better prose does**
- Uneven sentence length; a short sentence next to a long one.
- Specific numbers and names in place of adjectives.
- Plain "is"; concrete verbs; asides that do not land a punchline.
- Each claim stated once, where it is used.

Most frequent in this paper's drafts so far: colon reveals, "not X but Y", triads, and personified verbs (the
operator "pays", the index "gives", the sweep "costs"). Check those first.
