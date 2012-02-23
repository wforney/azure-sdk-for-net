﻿//-----------------------------------------------------------------------
// <copyright file="Task.cs" company="Microsoft">
//    Copyright 2011 Microsoft Corporation
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// </copyright>
// <summary>
//    Contains code for the Task[T] class.
// </summary>
//-----------------------------------------------------------------------

namespace Microsoft.WindowsAzure.StorageClient.Tasks
{
    using System;
    using System.Diagnostics;

    /// <summary>
    /// Represents an asynchronous computation that yields a resultToSet of type T.
    /// </summary>
    /// <typeparam name="T">The type of the resultToSet of the operation.</typeparam>
    /// <remarks>
    /// By this contract we:
    ///  1) guarantee that the completion routine is performed, regardless of the outcome of ExecuteStep.
    ///  2) insist that the completion routine does not throw an exception.
    ///  3) insists that the abort routine does not throw an exception.
    /// </remarks>
    internal abstract class Task<T> : ITask
    {
        /// <summary>
        /// The action to call once the operation is completed.
        /// </summary>
        private Action completionFunction;

        /// <summary>
        /// The resultToSet of the operation.
        /// </summary>
        private T result;

        /// <summary>
        /// Gets or sets the resultToSet of the operation and throws any exceptions raised by the operation.
        /// </summary>
        [DebuggerNonUserCode]
        public T Result
        {
            get
            {
                TraceHelper.WriteLine("Task, Result");
                if (!this.Completed)
                {
                    throw new InvalidOperationException("the operation is not complete");
                }

                if (this.Exception != null)
                {
                    throw this.Exception;
                }

                return this.result;
            }

            protected set
            {
                this.result = value;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether the operation was completed synchronously and therefore on main thread.
        /// </summary>
        [DebuggerNonUserCode]
        public bool CompletedSynchronously { [DebuggerStepThrough]get; protected set; }

        /// <summary>
        /// Gets or sets any exceptions raised during execution.
        /// </summary>
        [DebuggerNonUserCode]
        public Exception Exception { [DebuggerStepThrough]get; protected set; }

        /// <summary>
        /// Gets or sets a value indicating whether the task is completed.
        /// </summary>
        protected bool Completed { get; set; }

        /// <summary>
        /// Executes the tasks and waits for the resultToSet.
        /// </summary>
        /// <returns>The resultToSet of the operation.</returns>
        public T ExecuteAndWait()
        {
            TraceHelper.WriteLine("Task, ExecuteAndWait");

            // Potential optimization: Make sure that synchronous results are not creating events
            using (var evt = new System.Threading.ManualResetEvent(false))
            {
                this.ExecuteStep(() =>
                    {
                        Debug.Assert(evt != null, "evt != null");
                        evt.Set();
                    });
                evt.WaitOne();
            }

            return this.Result;
        }

        /// <summary>
        /// Executes a task, will not wait on a sync task.
        /// </summary>
        /// <returns>The resultToSet of the operation.</returns>
        public T Execute()
        {
            TraceHelper.WriteLine("Task, Execute");
            if (this is SynchronousTask<T>)
            {
                this.ExecuteInternal();
                return this.Result;
            }

            return this.ExecuteAndWait();
        }

        /// <summary>
        /// Executes a single step of the task. (Delegates to the concrete implemetation for specific step).
        /// </summary>
        /// <param name="cont">The completion function to be called.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.Design",
            "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "The exception is saved.")]
        [DebuggerNonUserCode]
        public void ExecuteStep(Action cont)
        {
            TraceHelper.WriteLine("Task, ExecuteStep");
            this.completionFunction = cont;
            try
            {
                this.ExecuteInternal();
            }
            catch (Exception ex)
            {
                this.Exception = ex;
                this.Complete(true);
            }
        }

        /// <summary>
        /// Implements an abort routine that fulfills the contract.
        /// </summary>
        [DebuggerNonUserCode]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Exception is saved or callbacks should never throw exception")]
        public void Abort()
        {
            TraceHelper.WriteLine("Task, Abort");
            lock (this)
            {
                if (this.Completed)
                {
                    // the task has already completed.
                    return;
                }

                this.Completed = true;
                this.CompletedSynchronously = false;
            }

            try
            {
                this.AbortInternal();
            }
            catch (Exception ex)
            {
                this.Exception = ex;
            }
            finally
            {
                // fulfill the callback contract (is definitely async)
                try
                {
                    if (this.completionFunction != null)
                    {
                        this.completionFunction();
                    }
                }
                catch (Exception ex)
                {
                    // the continuation itself threw an exception.  this is a violation of the contract!
                    Debug.Assert(false, "task: continuation threw exception: " + ex);
                }
            }
        }

        /// <summary>
        /// The specific implementation of the task's step.
        /// </summary>
        protected abstract void ExecuteInternal();

        /// <summary>
        /// Implements a safe way to obtain the resultToSet.
        /// </summary>
        /// <param name="resultToSet">The function used to get the resultToSet value.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.Design",
            "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "The exception is stored.")]
        [DebuggerNonUserCode]
        protected void SetResult(Func<T> resultToSet)
        {
            try
            {
                this.Result = resultToSet();
            }
            catch (AccessViolationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                this.Exception = ex;
            }
        }

        /// <summary>
        /// The task-specific abort that should be called.
        /// </summary>
        protected abstract void AbortInternal();

        /// <summary>
        /// The completion return that needs to be called whenever operation completes.
        /// </summary>
        /// <param name="completedSynchronouslyParam">Whether the underlying operation completed synchrnously.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.Design",
            "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Callbacks should never throw exception")]
        [DebuggerNonUserCode]
        protected void Complete(bool completedSynchronouslyParam)
        {
            TraceHelper.WriteLine("Task, Complete completedSynchronouslyParam {0}", completedSynchronouslyParam);
            lock (this)
            {
                if (this.Completed)
                {
                    // the task has already completed.  Perhaps it was aborted.
                    return;
                }

                this.CompletedSynchronously = completedSynchronouslyParam;
                this.Completed = true;
            }

            try
            {
                if (this.completionFunction != null)
                {
                    this.completionFunction();
                }
            }
            catch (Exception ex)
            {
                // the continuation itself threw an exception.  this is a violation of the contract!
                Debug.Assert(false, "task: continuation threw exception: " + ex);
            }
        }
    }
}
