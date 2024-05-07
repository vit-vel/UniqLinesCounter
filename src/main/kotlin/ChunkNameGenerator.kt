package org.example

import java.util.concurrent.atomic.AtomicInteger

interface ChunkNameGenerator {
    fun nextChunkName(): String
}

class SerialChunkNameGenerator(private val prefix: String) : ChunkNameGenerator {
    private val counter = AtomicInteger()

    override fun nextChunkName(): String =
        prefix + "-" + counter.getAndIncrement()
}