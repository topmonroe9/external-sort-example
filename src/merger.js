// merger.js - k-way merge implementation
const fs = require("fs");
const path = require("path");
const readline = require("readline");
const { createWriteStream } = require("fs");
const { promisify } = require("util");

// minheap implementation for efficient k-way merging
class MinHeap {
  constructor(compareFn = (a, b) => a.value.localeCompare(b.value)) {
    this.heap = [];
    this.compareFn = compareFn;
  }

  // get size of heap
  size() {
    return this.heap.length;
  }

  // insert element
  insert(element) {
    this.heap.push(element);
    this._siftUp(this.heap.length - 1);
  }

  // extract minimum element
  extractMin() {
    if (this.heap.length === 0) return null;

    const min = this.heap[0];
    const last = this.heap.pop();

    if (this.heap.length > 0) {
      this.heap[0] = last;
      this._siftDown(0);
    }

    return min;
  }

  // sift up an element at index
  _siftUp(index) {
    let parent = Math.floor((index - 1) / 2);

    while (
      index > 0 &&
      this.compareFn(this.heap[index], this.heap[parent]) < 0
    ) {
      // swap elements
      [this.heap[index], this.heap[parent]] = [
        this.heap[parent],
        this.heap[index],
      ];
      index = parent;
      parent = Math.floor((index - 1) / 2);
    }
  }

  // sift down an element at index
  _siftDown(index) {
    const left = 2 * index + 1;
    const right = 2 * index + 2;
    let smallest = index;

    if (
      left < this.heap.length &&
      this.compareFn(this.heap[left], this.heap[smallest]) < 0
    ) {
      smallest = left;
    }

    if (
      right < this.heap.length &&
      this.compareFn(this.heap[right], this.heap[smallest]) < 0
    ) {
      smallest = right;
    }

    if (smallest !== index) {
      // swap elements
      [this.heap[index], this.heap[smallest]] = [
        this.heap[smallest],
        this.heap[index],
      ];
      this._siftDown(smallest);
    }
  }
}

class Merger {
  /**
   * merge sorted chunks into a single output file
   * @param {string[]} sortedChunks - paths to sorted chunk files
   * @param {string} outputFile - path to the output file
   * @param {Object} options - merger options
   * @param {number} options.maxOpenFiles - maximum number of files to open simultaneously
   * @returns {Promise<void>}
   */
  async merge(sortedChunks, outputFile, options = {}) {
    // default options
    const maxOpenFiles = options.maxOpenFiles || 100;

    // use k-way merge if number of chunks is small enough
    if (sortedChunks.length <= maxOpenFiles) {
      await this._kWayMerge(sortedChunks, outputFile);
    } else {
      // use hierarchical merge for many chunks
      await this._hierarchicalMerge(sortedChunks, outputFile, maxOpenFiles);
    }
  }

  /**
   * perform a k-way merge of sorted chunks
   * @param {string[]} sortedChunks - paths to sorted chunk files
   * @param {string} outputFile - path to the output file
   * @returns {Promise<void>}
   * @private
   */
  async _kWayMerge(sortedChunks, outputFile) {
    console.log(`Performing k-way merge of ${sortedChunks.length} chunks...`);

    // check existence of all files before merging
    const existingChunks = [];
    for (const chunkPath of sortedChunks) {
      try {
        await promisify(fs.access)(chunkPath, fs.constants.R_OK);
        existingChunks.push(chunkPath);
      } catch (err) {
        console.warn(
          `warning: chunk file ${chunkPath} is not accessible and will be skipped: ${err.message}`
        );
      }
    }

    if (existingChunks.length === 0) {
      throw new Error("no valid chunk files found for merging");
    }

    console.log(`Found ${existingChunks.length} valid chunks for merging`);

    // create readers for each chunk
    const readers = [];
    for (const chunkPath of existingChunks) {
      try {
        const fileStream = fs.createReadStream(chunkPath);
        const rl = readline.createInterface({
          input: fileStream,
          crlfDelay: Infinity,
        });

        readers.push({
          path: chunkPath,
          reader: rl[Symbol.asyncIterator](),
          current: null,
          done: false,
        });
      } catch (err) {
        console.warn(
          `warning: could not create reader for ${chunkPath}: ${err.message}`
        );
      }
    }

    if (readers.length === 0) {
      throw new Error("could not create readers for any chunk files");
    }

    // initialize with first line from each reader
    for (const reader of readers) {
      try {
        const next = await reader.reader.next();
        reader.done = next.done;
        reader.current = next.done ? null : next.value;
      } catch (err) {
        console.warn(
          `warning: error reading from ${reader.path}: ${err.message}`
        );
        reader.done = true;
        reader.current = null;
      }
    }

    // create min heap with readers that have valid lines
    const heap = new MinHeap((a, b) => {
      // handle case when one of the values could be null
      if (a.current === null && b.current === null) return 0;
      if (a.current === null) return 1;
      if (b.current === null) return -1;

      // case-sensitive comparison
      if (a.current < b.current) return -1;
      if (a.current > b.current) return 1;
      return 0;
    });

    // add only readers with valid lines
    for (const reader of readers) {
      if (!reader.done && reader.current !== null) {
        heap.insert(reader);
      }
    }

    // create write stream for output
    const writeStream = createWriteStream(outputFile);

    // track progress
    let linesWritten = 0;
    const progressInterval = 1000000; // log progress every 1M lines

    try {
      // while we have lines to merge
      while (heap.size() > 0) {
        // get reader with smallest current line
        const reader = heap.extractMin();

        // write line to output
        writeStream.write(reader.current + "\n");
        linesWritten++;

        if (linesWritten % progressInterval === 0) {
          console.log(`Merged ${linesWritten.toLocaleString()} lines...`);
        }

        // get next line from this reader
        try {
          const next = await reader.reader.next();
          reader.done = next.done;
          reader.current = next.done ? null : next.value;

          // if reader has more lines, put it back in the heap
          if (!reader.done && reader.current !== null) {
            heap.insert(reader);
          }
        } catch (err) {
          console.warn(
            `warning: error reading next line from ${reader.path}: ${err.message}`
          );
          reader.done = true; // mark as done to avoid further attempts
        }
      }

      // close write stream
      await new Promise((resolve, reject) => {
        writeStream.end((err) => {
          if (err) reject(err);
          else resolve();
        });
      });

      // clean up readers
      for (const reader of readers) {
        try {
          if (reader.reader && typeof reader.reader.return === "function") {
            await reader.reader.return();
          }
        } catch (err) {
          console.warn(
            `warning: error closing reader for ${reader.path}: ${err.message}`
          );
        }
      }

      // clean up chunk files
      for (const chunkPath of existingChunks) {
        try {
          await promisify(fs.unlink)(chunkPath);
        } catch (err) {
          console.warn(
            `warning: could not delete chunk file ${chunkPath}: ${err.message}`
          );
        }
      }

      console.log(
        `k-way merge complete, wrote ${linesWritten.toLocaleString()} lines`
      );
    } catch (error) {
      // clean up in case of error
      writeStream.destroy();
      throw error;
    }
  }

  /**
   * perform a hierarchical merge for many chunks
   * @param {string[]} sortedChunks - paths to sorted chunk files
   * @param {string} outputFile - path to the output file
   * @param {number} maxOpenFiles - maximum number of files to open simultaneously
   * @returns {Promise<void>}
   * @private
   */
  async _hierarchicalMerge(sortedChunks, outputFile, maxOpenFiles) {
    console.log(
      `performing hierarchical merge of ${sortedChunks.length} chunks...`
    );

    // create temp directory for intermediate merges
    const tempDir = path.dirname(sortedChunks[0]);
    let currentLevel = sortedChunks;
    let levelIndex = 0;

    // keep merging chunks until we have only one
    while (currentLevel.length > 1) {
      levelIndex++;
      console.log(
        `hierarchical merge level ${levelIndex}: merging ${currentLevel.length} chunks...`
      );

      const nextLevel = [];

      // merge chunks in groups of maxOpenFiles
      for (let i = 0; i < currentLevel.length; i += maxOpenFiles) {
        const group = currentLevel.slice(i, i + maxOpenFiles);

        // if this is the last group and there's only one chunk, just pass it through
        if (group.length === 1 && i + maxOpenFiles >= currentLevel.length) {
          nextLevel.push(group[0]);
          continue;
        }

        // otherwise merge this group
        const mergedChunkPath = path.join(
          tempDir,
          `merged_level${levelIndex}_group${Math.floor(i / maxOpenFiles)}.txt`
        );

        await this._kWayMerge(group, mergedChunkPath);
        nextLevel.push(mergedChunkPath);
      }

      currentLevel = nextLevel;
    }

    // rename final merged file to output file
    if (currentLevel.length === 1 && currentLevel[0] !== outputFile) {
      await promisify(fs.rename)(currentLevel[0], outputFile);
    }

    console.log("hierarchical merge complete");
  }
}

module.exports = { Merger };
