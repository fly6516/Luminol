package me.earthme.luminol.threadregions;

import io.papermc.paper.threadedregions.ThreadedRegionizer;

import java.util.ArrayList;
import java.util.List;

public class NeoThreadRegionManager<R extends ThreadedRegionizer.ThreadedRegionData<R, S>, S extends ThreadedRegionizer.ThreadedRegionSectionData> {
    public List<ThreadedRegionizer<R, S>> regionizers = new ArrayList<>();
    public final io.papermc.paper.threadedregions.ThreadedRegionizer<io.papermc.paper.threadedregions.TickRegions.TickRegionData, io.papermc.paper.threadedregions.TickRegions.TickRegionSectionData> regioniser;

    {
        this.regioniser = new io.papermc.paper.threadedregions.ThreadedRegionizer<>(
                (int) Math.max(1L, (8L * 16L * 16L) / (1L << (2 * (io.papermc.paper.threadedregions.TickRegions.getRegionChunkShift())))),
                (1.0 / 6.0),
                Math.max(1, 8 / (1 << io.papermc.paper.threadedregions.TickRegions.getRegionChunkShift())),
                1,
                io.papermc.paper.threadedregions.TickRegions.getRegionChunkShift()
        );
    }

    public void addRegionizer(ThreadedRegionizer<R, S> regionizer) {
        this.regionizers.add(regionizer);
    }

    public void removeRegionizer(ThreadedRegionizer<R, S> regionizer) {
        this.regionizers.remove(regionizer);
    }

    public ThreadedRegionizer<R, S> getRegionizer() {
    }
}
