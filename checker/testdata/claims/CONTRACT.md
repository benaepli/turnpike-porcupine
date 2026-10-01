# Claim checking contract

This directory holds the fixtures shared by the two claim checkers, the Rust
engine in `spur-core` and the Go engine in `porcupine`. The same files live
in `spur/spur-core/tests/fixtures/claims/` and
`porcupine/checker/testdata/claims/`, byte for byte, this document included.
`scripts/tests/test_claim_fixtures.py` in the superproject enforces that.

This document is normative. Both engines implement it, and for every fixture
both must produce the expected verdict, reason, triage and witness, the
witness compared byte for byte. Where an implementation choice could change a
witness, the rule below decides it. A change to any rule here changes both
directories in one change, and every fixture it affects.

Everything below is a function of the history, the model and the claim. How
an engine stages its work (linearizability first, then the claim, then the
ladder), whether it memoizes, and how it hashes, never changes a verdict, a
triage or a witness.

## 1. Operations

### 1.1 Rows

A history is a sequence of rows. Every row has a kind and an action. A row
is a client operation row when both hold:

- its kind is `Invocation` or `Response`;
- its action ends in `Client.Write`, `Client.Read` or `Client.RMW`. In the
  executions table the action may carry a prefix; the kind of operation is
  taken from the suffix, `W`, `R` or `M` respectively.

Every other row is a system row: a row of any other kind (`Crash`,
`Recover`, `Partition`, `TimerFired` and so on), whatever its action, and an
`Invocation` or `Response` whose action has none of the three suffixes
(`Client.SimulateTimeout`, `Client.Delete`, `System.Crash` and so on). A row
is classified by its own kind and action alone. System rows are kept for
display only; the check ignores them, so they take no part in time (1.2),
sessions (1.3) or the adapter checks (1.4).

- An invocation of `W` or `M` carries a key (string) and a uid (integer).
- An invocation of `R` carries a key.
- A response of `R` carries the observed list; a response of `M` carries the
  prior list. A list is a sequence of integers.
- A response of `W` carries no value; anything it carries is ignored.
- A client id is a non-negative integer.

In the executions table an invocation's payload is `[destination, key]` for
`R` and `[destination, key, uid]` for `W` and `M`; the destination is not
read. A response of `R` or `M` has the payload `[list]`. An option holding a
list is read as that list; an empty option is no value. A payload of any
other length, or with an item of the wrong type, is malformed.

### 1.2 Time

Number the client operation rows (invocations and responses, system rows
excluded) 1, 2, 3, ... in row order. An operation's call time is its
invocation's number, its return time its response's number. Call and return
times are therefore all distinct.

An operation with no response is pending. A pending `W` or `M` is kept, with
return time one more than the largest row number. A pending `R` is dropped
after the session checks of 1.4.

### 1.3 Sessions

A session is one client id. A session's operations are ordered by call time;
an operation's index is its position in that order, from 0. Sessions are
ordered by client id, ascending, wherever an order is needed.

### 1.4 Adapter errors

The following make the check report verdict `unknown`, reason
`adapter_error` (an engine may append `: ` and a detail), triage `unknown`,
and no witness. They are checked before anything else.

- A `M` row when the model is `kv`.
- Two invocations with one operation id.
- A response with no open invocation of that id, or whose client or action
  differs from its invocation's. Actions are compared as whole strings,
  prefix included: `raft::Client.Write` and `Client.Write` differ.
- A missing or malformed key, uid or value (1.1). A response of `R` or `M`
  with no value is one.
- A negative client id.
- Two `W` or `M` operations with one uid.
- Within one session, an operation invoked before the previous operation of
  the session returned. A pending operation that is not the last of its
  session is such a case. Pending reads take part in this check.

## 2. Claims

### 2.1 Marks and names

A claim gives each operation kind the model declares a mark, `linearizable`
or `sequential`. The model `kv` declares `W` and `R`; `kv_rmw` declares `W`,
`R` and `M`. In JSON the claim is an object with the keys `Write`, `Read`
and, under `kv_rmw`, `RMW`, written in sorted key order.

Let `L` be the set of kinds marked linearizable. The claim's display name:

- `linearizable` when `L` holds every declared kind;
- `sequential` when `L` is empty;
- `ordered_sequential` when `L` is exactly the declared kinds other than `R`;
- `mixed` otherwise.

### 2.2 What a claim requires

A claim holds when some total order of the operations (pending reads
dropped) exists that:

- extends session order;
- places `a` before `b` whenever both kinds are in `L` and `a` returned
  before `b` was invoked (return time of `a` less than call time of `b`);
- is legal for the model (section 5).

### 2.3 The ladder

The ladder levels, strongest first, are the claims named `linearizable`,
`ordered_sequential` and `sequential` over the model's declared kinds. Each
implies the next. The triage of a history is found by walking down the
ladder: the first level that holds, `none` when every level fails, and
`unknown` when the walk meets an undecided level (section 6.5) before it
meets one that holds.

### 2.4 Partitioned mode

A claim whose `L` holds every declared kind is checked one key at a time:
the operations of each key form their own history, with sessions restricted
to that key, client ids unchanged. This depends on the claim only, never on
which kinds occur in the history. Every other claim is checked on the whole
history at once.

In partitioned mode the claim holds when every key's partition holds. Keys
are ordered by the bytes of their UTF-8 encoding. The witness of a failure
is taken from the first key in that order whose partition is `illegal`.

## 3. Value checks

Value checks run on the whole history, in every mode, before any search.
They examine every observing operation, that is every completed `R` and
every completed `M`, in call-time order, and stop at the first failure.
For one operation they apply, in this order:

1. `phantom_uid`: a uid in its list that is not the uid of a `W` (under
   `kv`) or of a `W` or `M` (under `kv_rmw`) of the same key, pending or not.
   `uids` lists the phantom uids without repetition, in order of first
   occurrence in the list.
2. `repeated_uid`: a uid that occurs twice or more in its list. `uids` lists
   them without repetition, in order of first occurrence.
3. Under `kv` only, `not_prefix`. Keep, per key, a longest list and the
   operation it came from, initially the empty list and no operation. If the
   operation's list is a prefix of the longest, continue. If the longest is a
   prefix of the operation's list, the operation's list becomes the longest.
   Otherwise fail: `uids` is the operation's whole list, `other` the
   operation the longest came from.
4. Under `kv_rmw` only, and only for an `M`, `same_prior`. Keep, per key and
   prior list, the first `M` seen with that prior. If one exists, fail:
   `uids` is the prior list, `other` that first `M`.

A failure is `illegal` under every claim, with triage `none` and a `value`
witness. `other` is null for `phantom_uid` and `repeated_uid`.

## 4. Edges

### 4.1 Relations and their order

An edge `a -> b` says `a` precedes `b` in every witness. Each edge has one of
five relations, ranked in this order:

1. `session`
2. `real_time`
3. `ww`
4. `wr`
5. `rw`

When several relations give the same ordered pair, the edge carries the
first of them in this order. Below, "the history" means the partition in
partitioned mode and the whole history otherwise.

### 4.2 Claim edges

- `session`: `a -> b` for every two operations of one session with `a`
  earlier. Every pair, not only consecutive ones.
- `real_time`: `a -> b` for every two operations whose kinds are both in `L`
  and where `a` returned before `b` was invoked. A pending operation is never
  the source of one.

### 4.3 Inferred edges under `kv`

Applied after the value checks pass. For each key `k`: let `v_1 .. v_m` be
the operations whose uids form the longest observed list of `k` (empty when
no read of `k` observed anything, or `k` has no reads), and `U_k` the `W`
operations of `k`, pending or not, whose uid no read of `k` observed.

- `ww`: `v_i -> v_(i+1)` for `1 <= i < m`, and `v_m -> u` for every `u` in
  `U_k` when `m > 0`.
- `wr`: `v_j -> r` for a read `r` of `k` whose list has length `j > 0`.
- `rw`: `r -> v_(j+1)` for a read `r` with a list of length `j < m`;
  `r -> u` for every `u` in `U_k` for a read with `j = m`.

Every legal order contains these edges, and every order that extends them,
session order and the claim's real-time edges is legal. So under `kv` the
claim holds exactly when these edges together are acyclic.

### 4.4 Inferred edges under `kv_rmw`

Applied after the value checks pass. For a list `l`, let `last(l)` be the
operation whose uid is the last element of `l`. For each key:

- `ww`: `last(p) -> m` for a completed `M` `m` whose prior `p` is not empty.
- `ww`: `m -> o` for a completed `M` `m` whose prior is empty and every other
  `W` or `M` `o` of the key.
- `wr`: `last(l) -> r` for a completed read `r` whose list `l` is not empty.
- `rw`: `r -> m` for a completed read `r` and a completed `M` `m` whose
  prior equals the read's list, the list not empty.
- `rw`: `r -> o` for a completed read `r` whose list is empty and every `W`
  or `M` `o` of the key.

An `M` whose prior ends with its own uid gets an edge to itself; that edge is
kept, and makes the operation never placeable.

## 5. Model steps

The state is, per key, a list of uids, initially empty.

- `kv`: `W` appends its uid. `R` is legal when its list equals the state.
- `kv_rmw`: `W` sets the state to its uid alone. A completed `M` is legal when
  its prior equals the state, and appends its uid. A pending `M` is always
  legal and appends its uid. `R` is legal when its list equals the state.

## 6. The search

### 6.1 Frontier and candidates

A frontier `f` gives, per session, how many of its operations are placed.
An operation is enabled at `f` when every operation with an edge into it
(4.2, 4.3, 4.4) is placed. Only session heads (the operation at index
`f[s]` of session `s`) can be enabled, since `session` edges cover the rest.

A candidate is an enabled head whose model step is legal in the current
state. Heads are tried in ascending call time, ties by ascending session id.

### 6.2 Depth-first search

```
visit(f, state):
    if every session is exhausted: success
    if (f, state) is in the memo: return failure
    read the interrupts if the expansion count is a multiple of 1024
    count one expansion
    if the number of placed operations is greater than at any earlier
        visit: record this visit as the deepest
    for each head o, in candidate order:
        if o is enabled and its step is legal:
            place o, apply the step
            if visit(next f, next state) succeeds: success
            undo
    add (f, state) to the memo
    return failure
```

The root is `visit(all zero, empty state)`. Its success is `ok`; its
failure is `illegal`.

### 6.3 The memo

A memo entry is the frontier and the state of every key, compared exactly.
It holds only visits whose every candidate was tried and failed; a visit
left by an interrupt is never added. Since enabling and legality depend only
on `f` and the state, a remembered visit would fail again, so the memo
changes neither the verdict, nor the first success, nor the deepest visit.
An engine may memoize by hash; a failure reached through a hashed memo is
then recomputed with the exact memo, and the witness comes from that pass.

### 6.4 The `kv` path

Under `kv` the search never backtracks. At each visit it takes the first
enabled head in candidate order (6.1), without testing any step, and only
then tests that head's step. If the step is legal the head is placed and the
search continues; if it is rejected, that is a checker defect, since with
the edges of 4.3 every enabled head is legal: verdict `unknown`, reason
`claim_internal`. No other head is tested. The search stops with `illegal`
at its first visit with operations left and no enabled head. With the edges
of 4.3 this visit is unique as a set of placed operations: it is every
operation no cycle reaches.

### 6.5 Interrupts

The deadline and the cancellation flag are read before the first expansion
of each search and then every 1024 expansions. A search interrupted by them
is undecided: reason `cancelled` when the flag is set, else `timeout`.
Value checks and edge construction come before the first read, so a value
failure is reported even when the deadline has already passed.

## 7. Verdict, reason and triage

- `ok`: the claim holds. Reason empty.
- `illegal`: the claim does not hold. Reason empty.
- `unknown`: undecided, with reason `adapter_error`, `timeout`, `cancelled`
  or `claim_internal`.

In partitioned mode an `illegal` partition makes the claim `illegal` even if
another partition is undecided. Otherwise any undecided partition makes it
`unknown`.

The triage follows 2.3. An engine may decide a ladder level by implication
(a level that holds implies every weaker one; a level that fails implies
every stronger one fails) and may reuse the claim's verdict when the claim
is a ladder level.

## 8. The witness

### 8.1 Shape

A witness is reported for every definitive verdict and is null for
`unknown`. Fields, in this order:

| Field | Value |
| --- | --- |
| `level` | the claim's display name (2.1) |
| `kind` | `order`, `cycle`, `frontier` or `value` |
| `key` | the failing partition's key for `cycle` and `frontier` in partitioned mode, else null |
| `order` | operation ids, see below |
| `cycle` | edges `{"from", "to", "relation"}`, empty unless `cycle` |
| `blocked` | blocked heads, empty unless `frontier` |
| `value` | `{"op", "reason", "uids", "other"}` for `value`, else null |

Operations are named by their operation id (`unique_id`) everywhere.

### 8.2 Kinds

- `order`: the verdict is `ok`. `order` is a full witness (8.3).
- `value`: a value check failed (section 3). `order` is empty.
- `cycle`: the model is `kv` and the search failed. `order` is the placed
  order at the stopping visit of 6.4; `cycle` is a shortest cycle (8.4).
- `frontier`: the model is `kv_rmw` and the search failed. `order` is the
  placed order at the deepest visit; `blocked` names its heads (8.5).

### 8.3 The success order

On the whole history, the order is the placement order of the first success
of 6.2.

In partitioned mode each key's search gives its own order `p_k`. The
reported order merges them: walk the whole history as in 6.1, where an
operation is enabled when every predecessor under `session`, `real_time`
over every pair of kinds, and the chain of consecutive operations of each
`p_k` is placed, and at each step place the enabled head that comes first in
candidate order. No model step is checked. The walk never blocks, because
the union of real-time order and per-key orders of a linearizable history is
acyclic.

### 8.4 The shortest cycle

Over the unplaced operations of the stopping visit, take every edge of
section 4 between two of them, each labelled per 4.1. The out-neighbours of
an operation are ordered by (relation rank, target id), ascending.

For each unplaced operation `s`, in ascending id, run a breadth-first search
from `s`: the queue starts with `s`, which is marked visited. Pop `x`; for
each out-neighbour `y` of `x` in order: if `y` is `s`, the cycle is the
tree path from `s` to `x` followed by `x -> s`, and the search from `s`
ends; else if `y` is unvisited, mark it, record `x` as its parent, and push
it. Keep the first cycle found that is strictly shorter than every earlier
one. Rotate the kept cycle to start at its smallest id; it already does,
since a shorter or equal cycle through a smaller id would have been found
first. The `cycle` field lists its edges in cycle order, each with its
relation.

### 8.5 Blocked heads

At the deepest visit, for each session with operations left, in ascending
session id, its head `o`:

- If some edge into `o` comes from an unplaced operation:
  `{"session", "op", "reason": "precedence", "after": [...]}`, where `after`
  lists every unplaced operation with an edge into `o`, ascending by id, as
  `{"op", "relation"}` with the relation of 4.1.
- Otherwise its step is rejected:
  `{"session", "op", "reason": "value", "state": [...], "observed": [...]}`,
  where `state` is the state of `o`'s key at that visit and `observed` is the
  read's list or the `M`'s prior.

Every head at the deepest visit is one of the two: an enabled head with a
legal step would lead to a deeper visit.

### 8.6 Canonical serialization

The witness is one line of JSON with no whitespace outside strings:

- object fields in the order given above, for `cycle` edges `from`, `to`,
  `relation`; for blocked heads `session`, `op`, `reason`, then `after`, or
  `state` then `observed`; for `after` entries `op`, `relation`; for `value`
  `op`, `reason`, `uids`, `other`;
- integers in decimal, `-` for negatives, no leading zeros, no exponent;
- empty lists as `[]`, absent values as `null`;
- strings in double quotes, with `"` as `\"`, `\` as `\\`, U+0008 as `\b`,
  U+000C as `\f`, U+000A as `\n`, U+000D as `\r`, U+0009 as `\t`, any other
  code point below U+0020 as `\u00` and two lowercase hex digits, and every
  other code point written as its UTF-8 bytes, unescaped. In particular `<`,
  `>`, `&`, U+2028 and U+2029 are not escaped, which a general JSON encoder
  may do by default.

Example, the store-buffer history under the sequential claim:

```
{"level":"sequential","kind":"cycle","key":null,"order":[],"cycle":[{"from":1,"to":2,"relation":"session"},{"from":2,"to":3,"relation":"rw"},{"from":3,"to":4,"relation":"session"},{"from":4,"to":1,"relation":"rw"}],"blocked":[],"value":null}
```

## 9. Fixtures

One JSON file per case, `<name>.json`. Every field is required.

| Field | Value |
| --- | --- |
| `name` | the file name without `.json` |
| `description` | one sentence on what the case shows |
| `model` | `kv` or `kv_rmw` |
| `claim` | the marks, as in 2.1 |
| `interrupt` | null, `deadline` (the deadline has passed when the check starts) or `cancel` (the cancellation flag is set when it starts) |
| `events` | the rows, in row order |
| `expect` | `verdict`, `reason`, `triage`, `witness` |

An event is one of:

```
{"kind": "Invocation", "id", "client", "action", "key", "uid", "step", "global_time"}
{"kind": "Response", "id", "client", "action", "value", "step", "global_time"}
{"kind": "Crash" | "Recover", "node", "step", "global_time"}
{"kind": <any other kind>, "node", "step", "global_time"}
```

A system event of any kind may also carry `action`, after `node`.

An `Invocation` or `Response` event whose action is a system action (1.1)
carries `id`, `client` and `action`, and `key`, `uid` or `value` only where
the case needs them. Otherwise `uid` appears only on `Client.Write` and
`Client.RMW` invocations, `value` only on `Client.Read` and `Client.RMW`
responses, and a case that shows a missing field leaves it out. `step` and `global_time`
are carried for display and do not enter the check.

`expect.reason` is the reason code of section 7, empty when definitive. An
engine's reason matches when it equals the code or starts with the code
followed by `: `. `expect.witness` is the canonical witness string, or null,
compared byte for byte.
