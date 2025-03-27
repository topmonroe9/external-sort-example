// splitter.js - file splitting logic
const fs = require("fs");
const path = require("path");
const readline = require("readline");
const { pipeline } = require("stream/promises");
const { createWriteStream } = require("fs");

class Splitter {
  /**
   * creates a new splitter instance
   * @param {Object} options - splitter options
   * @param {number} options.maxMemoryBytes - maximum memory to use per chunk (in bytes)
   */
  constructor(options = {}) {
    this.maxMemoryBytes = options.maxMemoryBytes || 400 * 1024 * 1024; // 400MB default
    this.chunkSizeBytes = Math.floor(this.maxMemoryBytes * 0.8); // use 80% of max memory for safety
  }

  /**
   * split a large file into smaller chunks
   * @param {string} inputFile - path to the input file
   * @param {string} tempDir - directory to store chunks
   * @returns {Promise<string[]>} - paths to the chunk files
   */
  async split(inputFile, tempDir) {
    const chunkFiles = [];
    let chunkIndex = 0;

    // create a buffer to accumulate lines until we reach the chunk size
    let currentChunkSize = 0;
    let linesBuffer = [];

    // create readline interface for line-by-line processing
    const fileStream = fs.createReadStream(inputFile, {
      highWaterMark: 1024 * 1024, // 1MB read buffer
    });

    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity,
    });

    // process each line
    for await (const line of rl) {
      // add line plus newline character to the buffer
      const lineSize = Buffer.byteLength(line + "\n");
      linesBuffer.push(line);
      currentChunkSize += lineSize;

      // if we've accumulated enough data, write the chunk
      if (currentChunkSize >= this.chunkSizeBytes) {
        await this._writeChunk(linesBuffer, tempDir, chunkIndex);
        chunkFiles.push(this._getChunkPath(tempDir, chunkIndex));

        // reset for next chunk
        linesBuffer = [];
        currentChunkSize = 0;
        chunkIndex++;

        // notify progress
        console.log(`Created chunk ${chunkIndex}`);
      }
    }

    // write any remaining lines
    if (linesBuffer.length > 0) {
      await this._writeChunk(linesBuffer, tempDir, chunkIndex);
      chunkFiles.push(this._getChunkPath(tempDir, chunkIndex));
      console.log(`Created final chunk ${chunkIndex + 1}`);
    }

    return chunkFiles;
  }

  /**
   * write a chunk of lines to a file
   * @param {string[]} lines - array of lines
   * @param {string} tempDir - directory to store chunks
   * @param {number} chunkIndex - current chunk index
   * @returns {Promise<void>}
   * @private
   */
  async _writeChunk(lines, tempDir, chunkIndex) {
    const chunkPath = this._getChunkPath(tempDir, chunkIndex);
    const writeStream = createWriteStream(chunkPath);

    for (const line of lines) {
      writeStream.write(line + "\n");
    }

    return new Promise((resolve, reject) => {
      writeStream.end((err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  /**
   * get the path for a chunk file
   * @param {string} tempDir - directory to store chunks
   * @param {number} chunkIndex - current chunk index
   * @returns {string} - path to the chunk file
   * @private
   */
  _getChunkPath(tempDir, chunkIndex) {
    return path.join(
      tempDir,
      `chunk_${String(chunkIndex).padStart(6, "0")}.txt`
    );
  }
}

module.exports = { Splitter };
