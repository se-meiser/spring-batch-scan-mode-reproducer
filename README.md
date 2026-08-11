# Spring Batch scan-mode reproducer

Spring Batch 6.0.4 reports a different write-skip exception for the deprecated `TaskletStep` path and the new `ChunkOrientedStep` path when scan mode reuses a rolled-back JPA entity.

## Run

```shell
mvn clean test
```

`taskletStepPathReportsTheOriginalBusinessException` passes. `chunkOrientedStepPathReportsTheOriginalBusinessException` intentionally fails: its `SkipListener` receives Hibernate's `PropertyValueException` saying the generated-id entity has an `uninitialized version value`, rather than `BusinessValidationException`.

Versions: Java 17 source/target, Spring Boot 4.1.0, and its managed `spring-batch-core` version. H2 is in-memory and no external service is required.

## Mechanism

The processor creates a `Product`; the writer persists and flushes it. Hibernate assigns its sequence id before `@PrePersist`, which throws for the invalid item; rollback leaves that Java object with an id and null `@Version`. During scan mode, `FaultTolerantChunkProcessor` re-invokes the processor and receives a fresh entity, while `ChunkOrientedStep` writes its cached output and Hibernate rejects the stale entity. Both step implementations ship in 6.0.4, so this is not a cross-version comparison. The only unavoidable builder API difference is legacy `.listener(...)` versus new `.skipListener(...)`.

Relevant upstream pointers: `ChunkOrientedStep#writeChunk`, `ChunkOrientedStep#processChunkSequentially`, `ChunkOrientedStep#scan`, and `FaultTolerantChunkProcessor#transform`.

Instrumentation records write-skip exceptions and processor calls per job. The legacy path calls the processor 4 times (three input items plus scan-mode reprocessing); the new path calls it 3 times. Both jobs use the same broad `Exception` skip rule so the listener can observe the replacement Hibernate exception; restricting it to the business exception causes the new path to fail rather than skip.

## Confirmation checks

Moving entity construction from the processor to the writer makes both paths receive the same fresh entity during scan mode and removes this divergence. Removing `@Version` changes Hibernate's stale-entity diagnostic/heuristic, so the `uninitialized version value` symptom is no longer present; cached-output reuse remains the underlying difference.
