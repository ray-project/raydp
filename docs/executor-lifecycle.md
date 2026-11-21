# Executor actor ownership and restarts

A Ray actor slot has a stable actor ID, while each restarted Spark executor gets
its own executor ID. The slot retains the handle returned when the actor was
created until the slot is released. `ExecutorActorSlot.handle` is immutable;
`addPendingRegisterExecutor` uses its handle argument only when creating a slot.

For Ray runtimes containing [Ray #45699](https://github.com/ray-project/ray/pull/45699),
named lookup handles do not acquire a keepalive reference. Replacing the
creation-time handle with a lookup wrapper can therefore release the last
counted reference when the original Java wrapper is collected. RayDP's default
Ray 2.1.0 dependency predates that change; runtime semantics must be checked
against the Ray version used by the application.

`ApplicationInfoTest.existingSlotRetainsItsCreationTimeHandle` checks that adding
a new executor generation with a different handle cannot replace the slot's
original handle. It checks Java identity, not native reference counting or GC.

Use JDK 17 and run the existing checks from `core/`:

```sh
mvn -pl raydp-main -am verify -Dscalastyle.includeTestSourceDirectory=true
```

Retiring an existing current generation when a restart arrives before its
previous disconnect is a separate known issue inherited from #487. This change
does not modify that transition or add an application-null guard.
