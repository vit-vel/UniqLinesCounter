package org.example

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlin.io.path.Path


/* Solution:
  * We can't keep the whole file in the RAM. So we have to process the file line by line.
  * To be able to remove all the duplicates while processing the file line by line,
  * we have to sort lines first.
  * Merge sort is the only way I can sort a file that doesn't fit in the memory.
  *
  * Step by step:
  * 1. Split the file into multiple chunks that fits in memory. Say 512Mb
  * 2. Run sorting of each chunk concurrently (could be started as soon as the first chunk is written to file)
  * 3. Merge files line by line to make a sorted file (could be started as soon as we have <mergeFactor> chunks sorted)
  *   3.1 Select Merge factor - how many files will be merged to a single file (e.g. 8)
  *   3.2 Remove duplicates while merging
  *   3.3 Remove merged files
  * 4. Repeat until the final merging step (number of chunks <= merge factor)
  * 5. On the final merging step it's enough just to count unique lines,
  *    we don't need to write the sorted result to a new file.
  *
  *
  *
  *  configurations that works for me:
  *  - -xmx4g; sortChunkSize = 128L * Mb; sortParallelism = 6; mergeFactor = 8
  *  - -xmx6g; sortChunkSize = 256L * Mb; sortParallelism = 6; mergeFactor = 8
  *
  *  Test: 120GB; line size: 14 symbols average
  *  Hardware: MacBook Pro 2019, 2.6 GHz 6-Core, 16Gb RAM, SSD
  *  Params: -xmx4g; sortChunkSize = 128L * Mb; sortParallelism = 6; mergeFactor = 8
  *  Time: 1h 11m
  *  Result: 1_000_000_000
 */

fun main() {

    // ========  Params  =========
    val inputPath = "data/example"

    // takes twice more in java heap because char takes 2 bytes
    val sortChunkSize: Long = 128L * MB

    // how many chunks are sorted simultaneously
    val sortParallelism = 6

    // how many files are merged to a single file
    val mergeFactor = 8
    // ==========================


    val basePath = Path("data")
    val chunkNameGenerator = SerialChunkNameGenerator("chunk")

    val sortExecutor: ExecutorService = Executors.newFixedThreadPool(sortParallelism)
    val mergeExecutor = Executors.newCachedThreadPool()

    val mergeSortActions = FileLinesMergeSortActions(chunkNameGenerator, basePath)
    val uniqLinesCounter =
        UniqLinesCounter(mergeSortActions, mergeSortActions, sortExecutor, mergeExecutor, mergeFactor)

    val result = uniqLinesCounter.run(inputPath, sortChunkSize).get()
    println("RESULT: $result")

    sortExecutor.shutdown()
    mergeExecutor.shutdown()
}

const val KB = 1024
const val MB = KB * 1024
const val GB = MB * 1024L
