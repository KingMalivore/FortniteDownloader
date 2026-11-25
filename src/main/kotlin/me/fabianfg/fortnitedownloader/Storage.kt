package me.fabianfg.fortnitedownloader

import me.fungames.jfortniteparse.ue4.reader.FArchive
import mu.KotlinLogging
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.internal.closeQuietly
import java.io.File
import java.io.InputStream
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import java.util.zip.InflaterInputStream
import kotlin.concurrent.withLock

private enum class ChunkStatus {
    Unavailable, // Readable from download
    Grabbing,    // Downloading
    Available,   // Readable from local copy
    Reading,     // Reading from local copy
    Readable     // Readable from memory
}

private class ChunkPoolData(
    var buffer : ByteArray? = null,
    val status : AtomicReference<ChunkStatus>
) {
    val mutex = ReentrantLock()
    val cv : Condition = mutex.newCondition()
}

/**
 * This class should not be used manually.
 * It is used by MountedBuild for storing and downloading chunks
 *
 */
class Storage internal constructor(val chunkPoolCapacity : Int, val cacheLocation : String, val cloudDir : String) {

    companion object {
        private val logger = KotlinLogging.logger("MountedBuild")
        private const val WAIT_RECHECK_SECONDS = 30L
    }

    init {
        File(cacheLocation).mkdirs()
    }

    private val client = OkHttpClient.Builder()
        .connectTimeout(20, TimeUnit.SECONDS)
        .build()
    private val chunkPool = HashMap<String, ChunkPoolData>(chunkPoolCapacity)
    private val chunkPoolMutex = ReentrantLock()

    fun getChunkPart(chunkPart: ChunkPart): ByteArray? {
        val chunk = getChunk(chunkPart.chunk)
        return if (chunk == null || chunkPart.offset == 0 && chunkPart.size == chunkPart.chunk.windowSize)
            chunk
        else {
            val partBuffer = ByteArray(chunkPart.size)
            chunk.copyInto(partBuffer, 0, chunkPart.offset, chunkPart.offset + chunkPart.size)
            partBuffer
        }
    }

    fun getChunk(chunk: Chunk): ByteArray? {
        val data = getPoolData(chunk)

        // The thread that claimed the chunk publishes the result, then wakes the waiters under the lock
        fun finish(buffer: ByteArray?, status: ChunkStatus) {
            data.buffer = buffer
            data.status.set(status)
            data.mutex.withLock {
                data.cv.signalAll()
            }
        }

        fun doDownload(): ByteArray? {
            val chunkData = downloadChunk(chunk)
            if (chunkData != null) finish(chunkData, ChunkStatus.Readable) else finish(null, ChunkStatus.Unavailable)
            return chunkData
        }

        // Runs the claimed work; if it throws, the chunk is released so waiters don't wait forever
        fun claimed(work: () -> ByteArray?): ByteArray? {
            try {
                return work()
            } finally {
                val status = data.status.get()
                if (status == ChunkStatus.Grabbing || status == ChunkStatus.Reading) finish(null, ChunkStatus.Unavailable)
            }
        }

        var waited = false
        while (true) {
            when (data.status.get()) {
                ChunkStatus.Readable -> return data.buffer
                ChunkStatus.Unavailable -> {
                    // A waiter whose chunk just failed gives up, as before, instead of retrying it in turn
                    if (waited) return null
                    // compareAndSet: only one thread downloads a chunk, the rest wait for it
                    if (data.status.compareAndSet(ChunkStatus.Unavailable, ChunkStatus.Grabbing)) {
                        return claimed { doDownload() }
                    }
                }
                ChunkStatus.Available -> {
                    if (data.status.compareAndSet(ChunkStatus.Available, ChunkStatus.Reading)) {
                        return claimed {
                            val chunkData = runCatching { File(cacheLocation + chunk.fileName()).readBytes() }.getOrNull()
                            if (chunkData == null || chunkData.size != chunk.windowSize) {
                                // That can / will happen if the chunk is very new on the file system
                                // It can also happen if the chunk file just got deleted
                                if (chunkData == null)
                                    logger.warn { "Failed to read chunk ${chunk.fileName()} from file, didn't receive anything. Why tho" }
                                else
                                    logger.warn { "Failed to read chunk ${chunk.fileName()} from file. Expected ${chunk.windowSize} bytes, got ${chunkData.size} bytes. Why tho" }
                                val downloaded = doDownload()
                                if (downloaded != null)
                                    logger.info { "Re-download of previously failed chunk ${chunk.fileName()} succeeded" }
                                else
                                    logger.info { "Re-download of previously failed chunk ${chunk.fileName()} also failed" }
                                downloaded
                            } else {
                                finish(chunkData, ChunkStatus.Readable)
                                chunkData
                            }
                        }
                    }
                }
                ChunkStatus.Grabbing, ChunkStatus.Reading -> {
                    waited = true
                    data.mutex.withLock {
                        // Recheck under the lock: a signal sent before this thread started waiting would otherwise be missed forever
                        while (data.status.get().let { it == ChunkStatus.Grabbing || it == ChunkStatus.Reading }) {
                            data.cv.await(WAIT_RECHECK_SECONDS, TimeUnit.SECONDS)
                        }
                    }
                }
            }
        }
    }

    private fun getPoolData(chunk: Chunk) = chunkPoolMutex.withLock {
        for (storedChunk in chunkPool) {
            if (storedChunk.key == chunk.guid)
                return@withLock storedChunk.value
        }

        while (chunkPool.size >= chunkPoolCapacity) {
            chunkPool.remove(chunkPool.keys.first())
        }

        val data = ChunkPoolData(
            null,
            AtomicReference(getUnpooledChunkStatus(chunk))
        )
        chunkPool[chunk.guid] = data
        data
    }

    private fun getUnpooledChunkStatus(chunk: Chunk) =
        if (isChunkDownloaded(chunk)) ChunkStatus.Available else ChunkStatus.Unavailable

    fun isChunkDownloaded(chunk: Chunk) =
        File(cacheLocation + chunk.fileName()).exists()

    @Suppress("EXPERIMENTAL_API_USAGE")
    fun downloadChunk(chunk: Chunk) = runCatching {
        val response = client.newCall(Request.Builder()
            .url(cloudDir + chunk.url())
            .addHeader("Accept-Encoding", "br, gzip, deflate")
            .build()).execute()
        val body = response.body
        if (response.isSuccessful && body != null) {
            val data = body.use {
                val chunkData = body.byteStream()
                val reader = FStreamArchive(chunkData)
                reader.skip(4)
                val version = reader.readInt32()
                val headerSize = reader.readInt32()
                reader.skip(28)
                val storedAs = reader.readUInt8().toInt()
                val decompressedSize = if (version >= 3) {
                    reader.skip(21)
                    reader.readInt32()
                }
                else
                    1024 * 1024
                reader.skip((headerSize - reader.pos()).toLong())

                if (storedAs and 0x02 != 0) {
                    throw UnsupportedOperationException("Encrypted chunks are not supported (yet)")
                }

                if (storedAs and 0x01 != 0) {
                    val out = ModByteArrayOutputStream(decompressedSize)
                    val decompress = InflaterInputStream(chunkData)
                    decompress.copyTo(out)
                    out.size()
                    out.toByteArray()
                } else {
                    val out = ModByteArrayOutputStream(chunk.windowSize)
                    chunkData.copyTo(out)
                    out.toByteArray()
                }
            }
            File(cacheLocation + chunk.fileName()).writeBytes(data)
            data
        } else {
            logger.warn { "Received response code ${response.code} for chunk ${response.request.url}" }
            response.closeQuietly()
            return@runCatching null
        }
    }.onFailure { logger.warn(it) { "Exception while attempting to download chunk ${cloudDir + chunk.url()}" } }.getOrNull()
}

@Suppress("EXPERIMENTAL_OVERRIDE", "EXPERIMENTAL_API_USAGE")
private class FStreamArchive(val input : InputStream) : FArchive() {
    override var littleEndian = true

    private var consumedBytes = 0

    override fun clone() = FStreamArchive(input)

    override fun pos() = consumedBytes

    override fun printError() = "FStreamArchive Info: pos $consumedBytes"

    override fun read(b: ByteArray, off: Int, len: Int): Int {
        val res = input.read(b, off, len)
        consumedBytes += res
        return res
    }

    override fun read(): Int {
        val res = input.read()
        if (res >= 0)
            consumedBytes++
        return res
    }

    override fun seek(pos: Int) {
       throw UnsupportedOperationException("Seeking not supported")
    }

    override fun size(): Int {
        throw UnsupportedOperationException("Size not supported")
    }

    override fun skip(n: Long): Long {
        val res = input.skip(n)
        consumedBytes += res.toInt()
        return res
    }

}