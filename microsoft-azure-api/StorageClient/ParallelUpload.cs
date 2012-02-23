﻿//-----------------------------------------------------------------------
// <copyright file="ParallelUpload.cs" company="Microsoft">
//    Copyright 2011 Microsoft Corporation
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//      http://www.apache.org/licenses/LICENSE-2.0
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// </copyright>
// <summary>
//    Contains code for the ParallelUpload class.
// </summary>
//-----------------------------------------------------------------------

namespace Microsoft.WindowsAzure.StorageClient
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Security.Cryptography;
    using System.Threading;

    using Microsoft.WindowsAzure.StorageClient.Protocol;
    using Microsoft.WindowsAzure.StorageClient.Tasks;

    using TaskSequence = System.Collections.Generic.IEnumerable<Microsoft.WindowsAzure.StorageClient.Tasks.ITask>;

    /// <summary>Class used to upload blocks for a blob in parallel.</summary>
    /// <remarks>The parallelism factor is configurable at the CloudBlobClient.</remarks>
    internal class ParallelUpload
    {
        #region Constants and Fields

        /// <summary>Stores the blob we're uploading.</summary>
        private readonly CloudBlockBlob blob;

        /// <summary>Stores the blob's hash.</summary>
        private readonly MD5 blobHash;

        /// <summary>The list of uploaded blocks.</summary>
        private readonly List<string> blockList;

        /// <summary>Stores the block size.</summary>
        private readonly long blockSize;

        /// <summary>Stores the request options to use.</summary>
        private readonly BlobRequestOptions options;

        /// <summary>Bound on number of parallel active tasks (threads).</summary>
        private readonly int parellelism;

        /// <summary>Stores the source stream to upload from.</summary>
        private readonly Stream sourceStream;

        /// <summary>Number of block upload tasks created.</summary>
        private int consumerTasksCreated;

        /// <summary>Number of dispenser calls.</summary>
        private int dispenserCallCount;

        /// <summary>Stores the dispensized stream size.</summary>
        private long dispensizedStreamSize;

        /// <summary>Number of block creation tasks.</summary>
        private int producerTasksCreated;

        #endregion

        #region Constructors and Destructors

        /// <summary>Initializes a new instance of the <see cref="ParallelUpload"/> class.</summary>
        /// <param name="source">The source stream. </param>
        /// <param name="options">The request options. </param>
        /// <param name="blockSize">The block size to use. </param>
        /// <param name="blob">The blob to upload to. </param>
        internal ParallelUpload(Stream source, BlobRequestOptions options, long blockSize, CloudBlockBlob blob)
        {
            this.sourceStream = source;
            this.blockSize = blockSize;
            this.options = options;
            this.dispensizedStreamSize = 0;
            this.blob = blob;
            this.blobHash = MD5.Create();
            this.blockList = new List<string>();
            this.parellelism = this.GetParallelismFactor();
        }

        #endregion

        #region Methods

        /// <summary>Perform a parallel upload of blocks for a blob from a given stream.</summary>
        /// <param name="uploadFunc">The upload func. </param>
        /// <returns>A <see cref="TaskSequence"/> that uploads the blob in parallel. </returns>
        /// <remarks>The operation is done as a series of alternate producer and consumer tasks. The producer tasks dispense out chunks of source stream as fixed size blocks. This is done in serial order on a thread using InvokeTaskSequence's serial execution. The consumer tasks upload each block in parallel on multiple thread. The producer thread waits for at least one consumer task to finish before adding more producer tasks. The producer thread quits when no more data can be read from the stream and no other pending consumer tasks.</remarks>
        internal TaskSequence ParallelExecute(
            Func<SmallBlockMemoryStream, string, string, BlobRequestOptions, Task<NullTaskReturn>> uploadFunc)
        {
            var moreToUpload = true;
            var asyncResults = new List<IAsyncResult>();

            var rand = new Random();
            var blockIdSequenceNumber = (long)rand.Next() << 32;
            blockIdSequenceNumber += rand.Next();

            do
            {
                var currentPendingTasks = asyncResults.Count;

                // Step 1 
                // Create producer tasks in a serial order as stream can only be read sequentially
                for (var i = currentPendingTasks; i < this.parellelism && moreToUpload; i++)
                {
                    string blockId = null;
                    string blockHash = null;
                    SmallBlockMemoryStream blockAsStream = null;
                    blockIdSequenceNumber++;

                    var number = blockIdSequenceNumber;
                    var producerTask =
                        new InvokeTaskSequenceTask(
                            () => this.DispenseBlockStream(
                                number, 
                                (stream, id, hashVal) =>
                                    {
                                        blockAsStream = stream;
                                        blockId = id;
                                        blockHash = hashVal;
                                    }));

                    yield return producerTask;

                    this.producerTasksCreated++;

                    var scatch = producerTask.Result;

                    if (blockAsStream == null)
                    {
                        TraceHelper.WriteLine("No more upload tasks");
                        moreToUpload = false;
                    }
                    else
                    {
                        // Step 2
                        // Fire off consumer tasks that may finish on other threads;                        
                        var task = uploadFunc(blockAsStream, blockId, blockHash, this.options);
                        var asyncresult = task.ToAsyncResult(null, null);
                        this.consumerTasksCreated++;

                        asyncResults.Add(asyncresult);
                    }
                }

                // Step 3
                // Wait for 1 or more consumer tasks to finish inorder to bound set of parallel tasks
                if (asyncResults.Count > 0)
                {
                    var waitTimeout = GetWaitTimeout(this.options);

                    TraceHelper.WriteLine("Starting wait");

                    var waitResult = WaitHandle.WaitAny(
                        asyncResults.Select(result => result.AsyncWaitHandle).ToArray(), waitTimeout);

                    TraceHelper.WriteLine("Ending wait");

                    if (waitResult == WaitHandle.WaitTimeout)
                    {
                        throw TimeoutHelper.ThrowTimeoutError(this.options.Timeout.GetValueOrDefault());
                    }

                    CompleteAsyncresult(asyncResults, waitResult);

                    // Optimize away any other completed tasks
                    for (var index = 0; index < asyncResults.Count; index++)
                    {
                        var result = asyncResults[index];
                        if (result.IsCompleted)
                        {
                            CompleteAsyncresult(asyncResults, index);
                            index--;
                        }
                    }
                }
            }
            while (moreToUpload || asyncResults.Count != 0);

            TraceHelper.WriteLine(
                "Total producer tasks created {0}, consumer tasks created {1} ", 
                this.producerTasksCreated, 
                this.consumerTasksCreated);

            var commitTask = TaskImplHelper.GetRetryableAsyncTask(this.CommitBlob, this.options.RetryPolicy);

            yield return commitTask;
            var commitTaskResult = commitTask.Result;
        }

        /// <summary>Completes the asyncresult.</summary>
        /// <param name="asyncResults">The async results. </param>
        /// <param name="index">The index. </param>
        private static void CompleteAsyncresult(List<IAsyncResult> asyncResults, int index)
        {
            var signalledResult = asyncResults[index];

            // NO locking necessary as they happen on the singleton producer thread
            asyncResults.RemoveAt(index);
            TaskImplHelper.EndImpl(signalledResult);
        }

        /// <summary>Gets the wait timeout.</summary>
        /// <param name="options">The options. </param>
        /// <returns>The wait timeout. </returns>
        private static int GetWaitTimeout(BlobRequestOptions options)
        {
            if (options.Timeout.HasValue)
            {
                return options.Timeout.RoundUpToMilliseconds();
            }

            return Timeout.Infinite;
        }

        /// <summary>As a final step upload the block list to commit the blob.</summary>
        /// <returns>A <see cref="TaskSequence"/> that commits the blob. </returns>
        private TaskSequence CommitBlob()
        {
            var hashValue = StreamUtilities.GetHashValue(this.blobHash);
            this.blob.Properties.ContentMD5 = hashValue;

            // At the convenience layer we always upload uncommitted blocks
            var putBlockList = this.blockList.Select(id => new PutBlockListItem(id, BlockSearchMode.Uncommitted)).ToList();

            return this.blob.UploadBlockList(putBlockList, this.options);
        }

        /// <summary>Upload a single block. This can happen on parallel threads.</summary>
        /// <param name="blockIdSequenceNumber">The block sequence prefix value. </param>
        /// <param name="setResult">The set result. </param>
        /// <returns>A <see cref="TaskSequence"/> that dispenses a block stream. </returns>
        private TaskSequence DispenseBlockStream(
            long blockIdSequenceNumber, Action<SmallBlockMemoryStream, string, string> setResult)
        {
            var currentCallIndex = this.dispenserCallCount++;
            TraceHelper.WriteLine("Staring dispensBlockStream for id {0}", currentCallIndex);

            var memoryStream = new SmallBlockMemoryStream(Constants.DefaultBufferSize);

            var md5Check = MD5.Create();
            int totalCopied = 0, numRead;

            do
            {
                var buffer = new byte[Constants.DefaultBufferSize];
                var numToRead = (int)Math.Min(buffer.Length, this.blockSize - totalCopied);
                var readTask = this.sourceStream.ReadAsync(buffer, 0, numToRead);
                yield return readTask;
                numRead = readTask.Result;

                if (numRead != 0)
                {
                    // Verify the content
                    StreamUtilities.ComputeHash(buffer, 0, numRead, md5Check);
                    StreamUtilities.ComputeHash(buffer, 0, numRead, this.blobHash);

                    var writeTask = memoryStream.WriteAsync(buffer, 0, numRead);
                    yield return writeTask;

                    // Materialize any exceptions
                    var scratch = writeTask.Result;

                    totalCopied += numRead;
                }
            }
            while (numRead != 0 && totalCopied < this.blockSize);

            // No locking necessary as only once active Dispense Task
            this.dispensizedStreamSize += totalCopied;

            if (totalCopied != 0)
            {
                var hashVal = StreamUtilities.GetHashValue(md5Check);
                var blockId = Utilities.GenerateBlockIDWithHash(hashVal, blockIdSequenceNumber);

                this.blockList.Add(blockId);

                memoryStream.Position = 0;
                setResult(memoryStream, blockId, hashVal);
            }
            else
            {
                memoryStream.Close();
                setResult(null, null, null);
            }

            TraceHelper.WriteLine("Ending dispensBlockStream for id {0}", currentCallIndex);
        }

        /// <summary>Gets the parallelism factor.</summary>
        /// <returns>The parallelism factor. </returns>
        private int GetParallelismFactor()
        {
            return this.blob.ServiceClient.ParallelOperationThreadCount;
        }

        #endregion
    }
}