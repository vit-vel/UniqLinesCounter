package org.example

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.Volatile

// todo: stop processing in case of exception
// todo: don't run merge sort if file leq chunk size
class UniqLinesCounter(
    private val mergeSortActions: ExternalMergeSortActions,
    private val uniqValuesCounter: UniqValuesCounter,
    private val sortExecutor: ExecutorService,
    private val mergeExecutor: ExecutorService,
    private val mergeFactor: Int
) {

    // used to check if no new chunks expected
    private val activeJobsCounter = AtomicInteger(0)

    @Volatile
    private var canceled = false

    private val readyChunks = ConcurrentLinkedQueue<String>()
    private val takeReadyChunksLock = Object()

    private val resultFuture: CompletableFuture<Long> = CompletableFuture()

    private val stageExceptionHandler: (th: Throwable) -> Unit = { th ->
        innerCancel()
        resultFuture.completeExceptionally(th)
    }

    fun run(inputPath: String, splitChunkMaxSize: Long): CompletableFuture<Long> {
        activeJobsCounter.incrementAndGet()
        CompletableFuture.supplyAsync {
            mergeSortActions.split(inputPath, splitChunkMaxSize, Int.MAX_VALUE.toLong()) { chunkName ->
                sortFileAsync(chunkName)
            }
            activeJobsCounter.decrementAndGet()
            Unit
        }.exceptionally(stageExceptionHandler)

        return resultFuture
    }

    /*
     * Not true cancel for now.
     * It doesn't cancel current sort and merge stages
     * but prevent running new stages
     */
    fun cancel() {
        innerCancel()
        resultFuture.cancel(true)
    }

    private fun innerCancel() {
        canceled = true
        mergeSortActions.clear()
    }

    private fun isCancelled() = canceled

    private fun sortFileAsync(chunkName: String) {
        activeJobsCounter.incrementAndGet()

        CompletableFuture.supplyAsync(
            {
                if (isCancelled()) activeJobsCounter.decrementAndGet()
                else {
                    val resultChunk = mergeSortActions.sort(chunkName)
                    readyChunks.add(resultChunk)
                    activeJobsCounter.decrementAndGet()
                    tryRunNextStep()
                }
            }, sortExecutor
        ).exceptionally(stageExceptionHandler)
    }

    private fun mergeAsync() {
        activeJobsCounter.incrementAndGet()
        val chunks = getChunksForMerging()
        chunks?.let { chunkNames ->
            CompletableFuture.supplyAsync(
                {
                    if (isCancelled()) activeJobsCounter.decrementAndGet()
                    else {
                        val resultChunk = mergeSortActions.merge(chunkNames)
                        readyChunks.add(resultChunk)
                        activeJobsCounter.decrementAndGet()
                        tryRunNextStep()
                    }
                }, mergeExecutor
            ).exceptionally(stageExceptionHandler)
        } ?: activeJobsCounter.decrementAndGet()
    }

    private fun tryRunNextStep() {
        val readyChunksSize = readyChunks.size
        when {
            readyChunksSize > mergeFactor -> mergeAsync()
            noMoreChunksExpected() -> countUniqAsync()
            readyChunksSize == mergeFactor -> mergeAsync()
            else -> Unit
        }
    }

    private fun countUniqAsync() {
        if (!resultFuture.isDone && readyChunks.size > 0) {
            var chunks: Array<String>? = null
            synchronized(takeReadyChunksLock) {
                if (readyChunks.size > 0) {
                    chunks = readyChunks.toTypedArray()
                    readyChunks.clear()
                }
            }
            chunks?.let { chunkNames ->
                if (chunkNames.size > mergeFactor) {
                    error("COUNT UNIQ: accepted more chunks than mergeFactor: ${chunkNames.size} > $mergeFactor")
                }
                CompletableFuture.supplyAsync(
                    {
                        if (!isCancelled()) {
                            val result = uniqValuesCounter.countUniq(chunkNames)
                            resultFuture.complete(result)
                        }
                    }, mergeExecutor
                ).exceptionally(stageExceptionHandler)
            }
        }
    }

    private fun getChunksForMerging(): Array<String>? {
        var chunks: Array<String>? = null

        if (readyChunks.size >= mergeFactor) {
            // only one thread at the time can take ready chunks
            // in other case, two threads may take only half and no one can run merge
            synchronized(takeReadyChunksLock) {
                // double check if it's still enough ready chunks for merging
                if (readyChunks.size >= mergeFactor) {
                    chunks = Array(mergeFactor) { readyChunks.poll() }
                }
            }
        }
        return chunks
    }

    private fun noMoreChunksExpected() = activeJobsCounter.get() == 0
}