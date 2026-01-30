# Frequently Asked Questions

> **Purpose**: Consolidated FAQ index for Stellar Full History RPC Service design documentation  
> **Sources**: [Meta Store Design](./02-meta-store-design.md), [Checkpointing and Transitions](./10-checkpointing-and-transitions.md)

---

## Quick Answers

| Question | Short Answer | Full Answer |
|----------|--------------|-------------|
| What is a Range ID? | Partition index for 10M ledgers | [Doc 10](./10-checkpointing-and-transitions.md#faq-section) |
| Why separate Ledger and TxHash phases? | Different durations, parallel execution | [Doc 02](./02-meta-store-design.md#frequently-asked-questions) |
| What happens during graceful shutdown? | Checkpoint and exit cleanly, resume seamlessly | [Doc 02](./02-meta-store-design.md#frequently-asked-questions) |
| Can I query during transition? | Yes, active stores remain accessible | [Doc 02](./02-meta-store-design.md#frequently-asked-questions), [Doc 10](./10-checkpointing-and-transitions.md#faq-section) |
| What if process crashes during transition? | Crash-safe, resumes from phase checkpoints | [Doc 02](./02-meta-store-design.md#frequently-asked-questions) |
| Are range boundaries inclusive? | Yes, both ends inclusive | [Doc 10](./10-checkpointing-and-transitions.md#faq-section) |
| What ledgers get re-ingested after crash? | From last_committed_ledger + 1 to crash point | [Doc 10](./10-checkpointing-and-transitions.md#faq-section) |
| How does service know backfill vs streaming? | Startup flags and meta store state | [Doc 02](./02-meta-store-design.md#frequently-asked-questions) |

---

## By Topic

### Operating Modes

- [How does the service know to start in streaming vs backfill mode?](./02-meta-store-design.md#frequently-asked-questions)
- [What happens during graceful shutdown?](./02-meta-store-design.md#frequently-asked-questions)

### Meta Store Design

- [Why are there separate Ledger and TxHash phases?](./02-meta-store-design.md#frequently-asked-questions)
- [Why is cf_counts stored as JSON instead of separate keys?](./02-meta-store-design.md#frequently-asked-questions)
- [Why does checkpoint data never get deleted?](./02-meta-store-design.md#frequently-asked-questions)
- [What's the difference between last_committed_ledger and count?](./02-meta-store-design.md#frequently-asked-questions)

### Crash Recovery and Transitions

- [What happens if the process crashes during TRANSITIONING state?](./02-meta-store-design.md#frequently-asked-questions)
- [Can I query data while a range is TRANSITIONING?](./02-meta-store-design.md#frequently-asked-questions)
- [If I crash at ledger 7,500,000 with last_committed_ledger = 7499001, what ledgers get re-ingested?](./10-checkpointing-and-transitions.md#faq-section)
- [What happens if I query ledger 10,000,001 during the transition?](./10-checkpointing-and-transitions.md#faq-section)

### Range Boundaries and Checkpoints

- [What is the exact last ledger in Range 0 immutable store?](./10-checkpointing-and-transitions.md#faq-section)
- [Which store handles getLedgerBySequence(10000001)?](./10-checkpointing-and-transitions.md#faq-section)
- [Which store handles getLedgerBySequence(10000002)?](./10-checkpointing-and-transitions.md#faq-section)
- [What ledgers are in LFS chunk chunks/0000/000999.index?](./10-checkpointing-and-transitions.md#faq-section)
- [When does the transition to immutable store happen?](./10-checkpointing-and-transitions.md#faq-section)
- [Are range boundaries inclusive or exclusive?](./10-checkpointing-and-transitions.md#faq-section)
- [Why is the checkpoint formula (ledgerSeq - 1) % 1000 == 0 instead of ledgerSeq % 1000 == 0?](./10-checkpointing-and-transitions.md#faq-section)

---

## All FAQs by Document

### Meta Store Design (02-meta-store-design.md)

1. [Why are there separate Ledger and TxHash phases?](./02-meta-store-design.md#frequently-asked-questions)
2. [What happens if the process crashes during TRANSITIONING state?](./02-meta-store-design.md#frequently-asked-questions)
3. [Why is cf_counts stored as JSON instead of separate keys?](./02-meta-store-design.md#frequently-asked-questions)
4. [Can I query data while a range is TRANSITIONING?](./02-meta-store-design.md#frequently-asked-questions)
5. [Why does checkpoint data never get deleted?](./02-meta-store-design.md#frequently-asked-questions)
6. [What's the difference between last_committed_ledger and count?](./02-meta-store-design.md#frequently-asked-questions)
7. [What happens during graceful shutdown?](./02-meta-store-design.md#frequently-asked-questions)
8. [How does the service know to start in streaming vs backfill mode?](./02-meta-store-design.md#frequently-asked-questions)

### Checkpointing and Transitions (10-checkpointing-and-transitions.md)

1. [What is the exact last ledger in Range 0 immutable store?](./10-checkpointing-and-transitions.md#faq-section)
2. [Which store handles getLedgerBySequence(10000001)?](./10-checkpointing-and-transitions.md#faq-section)
3. [Which store handles getLedgerBySequence(10000002)?](./10-checkpointing-and-transitions.md#faq-section)
4. [What ledgers are in LFS chunk chunks/0000/000999.index?](./10-checkpointing-and-transitions.md#faq-section)
5. [When does the transition to immutable store happen?](./10-checkpointing-and-transitions.md#faq-section)
6. [If I crash at ledger 7,500,000 with last_committed_ledger = 7499001, what ledgers get re-ingested?](./10-checkpointing-and-transitions.md#faq-section)
7. [Why is the checkpoint formula (ledgerSeq - 1) % 1000 == 0 instead of ledgerSeq % 1000 == 0?](./10-checkpointing-and-transitions.md#faq-section)
8. [Are range boundaries inclusive or exclusive?](./10-checkpointing-and-transitions.md#faq-section)
9. [What happens if I query ledger 10,000,001 during the transition?](./10-checkpointing-and-transitions.md#faq-section)

---

## Contributing

Found a question that should be here? Please add it to the appropriate design document's FAQ section, then update this consolidated index.
