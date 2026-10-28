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
