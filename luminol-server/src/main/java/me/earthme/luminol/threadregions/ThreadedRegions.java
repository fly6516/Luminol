package me.earthme.luminol.threadregions;

import ca.spottedleaf.concurrentutil.map.SWMRLong2ObjectHashTable;
import io.papermc.paper.threadedregions.ThreadedRegionizer;
import net.minecraft.server.level.ServerLevel;

public class ThreadedRegions<R extends ThreadedRegionizer.ThreadedRegionData<R, S>, S extends ThreadedRegionizer.ThreadedRegionSectionData> {
    public final ServerLevel world;
    public final SWMRLong2ObjectHashTable<ThreadedRegionizer.ThreadedRegionSection<R, S>> sections = new SWMRLong2ObjectHashTable<>();
    public final SWMRLong2ObjectHashTable<ThreadedRegionizer.ThreadedRegion<R, S>> regionsById = new SWMRLong2ObjectHashTable<>();
    public final ThreadedRegionizer.RegionCallbacks<R, S> callbacks;

    public ThreadedRegions(ServerLevel world, ThreadedRegionizer.RegionCallbacks<R, S> callback) {
        this.world = world;
        this.callbacks = callback;
    }
}
