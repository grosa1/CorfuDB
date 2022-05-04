package org.corfudb.runtime.object;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.collections.ICorfuImmutable;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.ref.Reference;
import java.util.Map;
import java.util.function.LongConsumer;

@NotThreadSafe
@Slf4j
public class SnapshotProxyAdapter<T extends ICorfuSMR<T>, O extends ICorfuImmutable<T>> {

    private final Reference<O> snapshotReference;

    private final long baseSnapshotVersion;

    private final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;

    public SnapshotProxyAdapter(@NonNull final Reference<O> snapshotReference, final long snapshotVersion,
                                @NonNull final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap) {
        this.snapshotReference = snapshotReference;
        this.baseSnapshotVersion = snapshotVersion;
        this.upcallTargetMap = upcallTargetMap;
    }

    public <R> R access(@NonNull ICorfuSMRAccess<R, T> accessFunction, @NonNull LongConsumer versionAccessed) {
        ICorfuImmutable<T> immutableState = snapshotReference.get();

        if (immutableState == null) {
            // TODO: Throw custom exception.
            throw new RuntimeException("Snapshot reference not available");
        }

        final T wrappedObject = immutableState.getWrapper();
        final R ret = accessFunction.access(wrappedObject);
        versionAccessed.accept(baseSnapshotVersion);
        return ret;
    }

    public void logUpdate(@NonNull SMREntry updateEntry) {
        final ICorfuSMRUpcallTarget<T> target = upcallTargetMap.get(updateEntry.getSMRMethod());

        if (target == null) {
            throw new RuntimeException("Unknown upcall " + updateEntry.getSMRMethod());
        }

        ICorfuImmutable<T> immutableState = snapshotReference.get();

        if (immutableState == null) {
            // TODO: Throw custom exception.
            throw new RuntimeException("Snapshot reference not available");
        }

        final T wrappedObjecty = immutableState.getWrapper();
        target.upcall(wrappedObjecty, updateEntry.getSMRArguments());
    }

    // TODO: getUpcall().
}
