package org.example

import java.nio.file.InvalidPathException
import java.nio.file.Path
import kotlin.io.path.*

interface ExternalMergeSortActions {
    /**
     * calls action as soon as new chunk is ready
     */
    fun split(
        inputLocation: String,
        chunkMaxSize: Long,
        chunkMaxValuesNumber: Long = Int.MAX_VALUE.toLong(),
        action: (chunkName: String) -> Unit
    )

    /**
     * sorts the chunk and returns name of sorted chunk
     */
    fun sort(chunkName: String): String

    /**
     * merges chunks and returns name of new chunk
     */
    fun merge(chunkNames: Array<String>): String

    /**
     * clears tmp data
     */
    fun clear()
}

interface UniqValuesCounter {
    fun countUniq(chunkName: String): Long = countUniq(arrayOf(chunkName))
    fun countUniq(chunkNames: Array<String>): Long
}

/**
 * External MergeSort algorithm using files.
 * Merge stage deletes duplicates
 * Each method of the class may throw IOException
 */
class FileLinesMergeSortActions(
    private val chunkNameGenerator: ChunkNameGenerator,
    basePath: Path
) : ExternalMergeSortActions, UniqValuesCounter {

    private val tmpPath: Path by lazy {
        basePath.resolve("tmp").createDirectories()
    }

    private val chunkPath = { name: String -> tmpPath.resolve(name) }
    private val nextChunkPath = { chunkPath(chunkNameGenerator.nextChunkName()) }


    override fun split(
        inputLocation: String,
        chunkMaxSize: Long,
        chunkMaxValuesNumber: Long,
        action: (chunkName: String) -> Unit
    ) {
        println("SPLIT: Start splitting the file $inputLocation into chunks of size $chunkMaxSize bytes")

        val inputPath: Path = try {
            Path.of(inputLocation)
        } catch (ex: InvalidPathException) {
            throw IllegalArgumentException("Can't parse input location $inputLocation", ex)
        }

        // impossible to create array longer int_max (for further in memory sorting)
        var lineRemains = chunkMaxValuesNumber
        var chunkSizeRemains = chunkMaxSize
        var currentChunkPath = nextChunkPath()
        var writer = currentChunkPath.bufferedWriter()

        try {
            inputPath.forEachLine { line ->
                chunkSizeRemains -= line.length
                lineRemains -= 1
                if (chunkSizeRemains < 0 || lineRemains < 0) {
                    writer.close()
                    action(currentChunkPath.name)
                    currentChunkPath = nextChunkPath()
                    writer = currentChunkPath.bufferedWriter()
                    chunkSizeRemains = chunkMaxSize - line.length
                    lineRemains = chunkMaxValuesNumber - 1
                }
                writer.appendLine(line)
            }

            writer.close()
            action(currentChunkPath.name)
        } finally {
            writer.close()
        }
        println("SPLIT: Finish splitting the file $inputLocation")
    }

    /*
     * reads file to RAM, sorts and writes to the same file
     */
    override fun sort(chunkName: String): String {
        println("SORT: start sorting chunk $chunkName")

        val path = chunkPath(chunkName)
        var lines: Array<String>? = null

        path.bufferedReader().use { reader ->
            // build stream of lines, transform to array and sort in-place
            lines = reader.lines().toArray { size -> arrayOfNulls<String>(size) }
            lines?.sort()
        }

        path.bufferedWriter().use { writer ->
            lines?.forEach { writer.appendLine(it) }
        }

        println("SORT: chunk $chunkName has been sorted")
        return chunkName
    }

    override fun merge(chunkNames: Array<String>): String {
        val outputChunkPath = nextChunkPath()
        val outputChunkName = outputChunkPath.name

        println("MERGE: start merging chunks: ${chunkNames.joinToString()} to the output chunk $outputChunkName")

        outputChunkPath.bufferedWriter().use { writer ->
            innerMergeCustom(chunkNames) { line -> writer.appendLine(line) }
        }

        deleteChunks(chunkNames)

        println("MERGE: finish merging chunks ${chunkNames.joinToString()} to the output chunk $outputChunkName")
        return outputChunkName
    }

    @OptIn(ExperimentalPathApi::class)
    override fun clear() {
        tmpPath.deleteRecursively()
    }

    override fun countUniq(chunkNames: Array<String>): Long {
        println("COUNT UNIQ: start counting uniq lines from the chunk ${chunkNames.joinToString()}")

        var count: Long = 0
        innerMergeCustom(chunkNames) { _ -> count += 1 }

        deleteChunks(chunkNames)

        println("COUNT UNIQ: finish counting uniq lines from files ${chunkNames.joinToString()}")
        return count
    }


    /**
     * Merges sorted chunks
     * @param lineAction is called for each uniq line within merge. E.g. write to file
     * @param lastBlockAction is called in the end of merging for the last chunk when other chunks are over.
     */
    private fun innerMergeCustom(
        chunkNames: Array<String>,
        lineAction: (String) -> Unit
    ) {
        val chunkPaths = chunkNames.map { chunkPath(it) }
        val readers = chunkPaths.map { it.bufferedReader() }

        try {
            // init first lines of each chunk
            val lines = Array<String?>(readers.size) { i -> readers[i].readLine() }

            var lastWrittenLine: String? = null
            val processLineIfUniq = { line: String? ->
                if (lastWrittenLine != line && line != null) {
                    lineAction(line)
                    lastWrittenLine = line
                }
            }

            val linesIndices = lines.indices
            // number of readers that still have lines
            var readersRemains = lines.count { it != null }

            val linesByIdxComparator = compareBy<Int, String?>(nullsLast<String>()) { lines[it] }

            while (readersRemains > 1) {
                val minLineIdx: Int = linesIndices.minWith(linesByIdxComparator)

                processLineIfUniq(lines[minLineIdx])

                val nextLine = readers[minLineIdx].readLine()
                lines[minLineIdx] = nextLine
                if (nextLine == null) {
                    readersRemains -= 1
                    readers[minLineIdx].close()
                }
            }

            // remains only 1 reader, let's process all values
            val lastReaderIdx = linesIndices.find { lines[it] != null }
            lastReaderIdx?.let { idx ->
                processLineIfUniq(lines[idx])
                readers[idx].lineSequence().forEach(processLineIfUniq)
                readers[idx].close()
            }
        } finally {
            readers.forEach { it.close() }
        }
    }

    private fun deleteChunks(chunkNames: Array<String>) =
        chunkNames.forEach { chunkPath(it).deleteIfExists() }
}