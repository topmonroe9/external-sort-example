// sorter.js - in-memory sorting logic
const fs = require("fs");
const path = require("path");
const readline = require("readline");
const { promisify } = require("util");
const { Worker } = require("worker_threads");

class Sorter {
  /**
   * sort an array of lines
   * @param {string[]} lines - array of lines to sort
   * @returns {string[]} - sorted array of lines
   */
  sortLines(lines) {
    // use case-sensitive sorting
    return lines.sort((a, b) => {
      // direct string comparison (case-sensitive)
      if (a < b) return -1;
      if (a > b) return 1;
      return 0;
    });
  }

  /**
   * sort a chunk file
   * @param {string} chunkPath - path to the chunk file
   * @returns {Promise<string>} - path to the sorted chunk file
   */
  async sortChunk(chunkPath) {
    const lines = [];

    // read all lines from the chunk file
    const fileStream = fs.createReadStream(chunkPath);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity,
    });

    for await (const line of rl) {
      lines.push(line);
    }

    // sort lines in memory
    const sortedLines = this.sortLines(lines);

    // write sorted lines back to a new file
    const sortedChunkPath = chunkPath.replace(".txt", ".sorted.txt");
    const writeStream = fs.createWriteStream(sortedChunkPath);

    for (const line of sortedLines) {
      writeStream.write(line + "\n");
    }

    await new Promise((resolve, reject) => {
      writeStream.end((err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    // try to delete the original chunk to save disk space, but don't fail if unsuccessful
    try {
      await promisify(fs.unlink)(chunkPath);
    } catch (err) {
      console.warn(
        `warning: could not delete original chunk file ${chunkPath}: ${err.message}`
      );
      // continue processing despite the error
    }

    return sortedChunkPath;
  }

  /**
   * sort multiple chunk files, optionally using worker threads for parallelism
   * @param {string[]} chunkFiles - array of paths to chunk files
   * @param {Object} options - options for sorting
   * @param {boolean} options.useWorkers - whether to use worker threads for parallelism
   * @param {number} options.maxWorkers - maximum number of worker threads to use
   * @returns {Promise<string[]>} - array of paths to sorted chunk files
   */
  async sortChunks(chunkFiles, options = {}) {
    const useWorkers =
      options.useWorkers !== undefined ? options.useWorkers : true;
    const maxWorkers =
      options.maxWorkers ||
      Math.max(1, Math.min(4, require("os").cpus().length));

    if (!useWorkers || chunkFiles.length <= 1) {
      // simple sequential sorting for small number of chunks
      const sortedChunks = [];
      for (let i = 0; i < chunkFiles.length; i++) {
        const chunkPath = chunkFiles[i];
        console.log(`Sorting chunk ${i + 1}/${chunkFiles.length}...`);
        const sortedChunkPath = await this.sortChunk(chunkPath);
        sortedChunks.push(sortedChunkPath);
      }
      return sortedChunks;
    } else {
      // use worker threads for parallel sorting
      console.log(`Using ${maxWorkers} worker threads for parallel sorting`);

      // create a pool of workers
      const workerPool = [];
      for (let i = 0; i < maxWorkers; i++) {
        workerPool.push({
          id: i,
          busy: false,
          worker: null,
        });
      }

      const results = [];
      let completedChunks = 0;

      // process chunks with worker pool
      return new Promise((resolve, reject) => {
        const processNextChunk = () => {
          if (completedChunks === chunkFiles.length) {
            resolve(results);
            return;
          }

          const availableWorker = workerPool.find((w) => !w.busy);
          if (!availableWorker) return;

          const chunkIndex = completedChunks;
          if (chunkIndex >= chunkFiles.length) return;

          const chunkPath = chunkFiles[chunkIndex];
          availableWorker.busy = true;

          console.log(
            `Worker ${availableWorker.id} sorting chunk ${chunkIndex + 1}/${
              chunkFiles.length
            }...`
          );

          // create worker using external script file
          const workerPath = path.join(__dirname, "sorter-worker.js");
          const worker = new Worker(workerPath, {
            workerData: { chunkPath },
          });

          worker.on("message", (message) => {
            if (message.success) {
              results[chunkIndex] = message.sortedChunkPath;
              completedChunks++;
              availableWorker.busy = false;
              worker.terminate();
              processNextChunk();
            } else {
              reject(new Error(message.error));
            }
          });

          worker.on("error", (error) => {
            reject(error);
          });
        };

        // start initial workers
        for (let i = 0; i < maxWorkers && i < chunkFiles.length; i++) {
          processNextChunk();
        }
      });
    }
  }
}

module.exports = { Sorter };
