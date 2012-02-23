//-----------------------------------------------------------------------
// <copyright file="TableServiceContext.cs" company="Microsoft">
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
//    Contains code for the TableServiceContext class.
// </summary>
//-----------------------------------------------------------------------

namespace Microsoft.WindowsAzure.StorageClient
{
    using System;
    using System.Data.Services.Client;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Net;

    using Microsoft.WindowsAzure.StorageClient.Protocol;
    using Microsoft.WindowsAzure.StorageClient.Tasks;

    using TaskSequence = System.Collections.Generic.IEnumerable<Microsoft.WindowsAzure.StorageClient.Tasks.ITask>;

    /// <summary>Represents a <see cref="DataServiceContext"/> object for use with the Windows Azure Table service.</summary>
    public class TableServiceContext : DataServiceContext
    {
        #region Constructors and Destructors

        /// <summary>Initializes a new instance of the <see cref="TableServiceContext"/> class.</summary>
        /// <param name="baseAddress">The Table service endpoint to use create the service context. </param>
        /// <param name="credentials">The account credentials. </param>
        public TableServiceContext(string baseAddress, StorageCredentials credentials)
            : base(new Uri(baseAddress))
        {
            if (string.IsNullOrEmpty(baseAddress))
            {
                throw new ArgumentNullException("baseAddress");
            }

            if (credentials == null)
            {
                throw new ArgumentNullException("credentials");
            }

            if ((!credentials.CanSignRequest) || (!credentials.CanSignRequestLite))
            {
                throw new ArgumentException(SR.CredentialsCantSignRequest, "credentials");
            }

            this.SendingRequest += this.DataContextSendingRequest;

            this.StorageCredentials = credentials;
            this.IgnoreMissingProperties = true;
            this.MergeOption = MergeOption.PreserveChanges;

            this.RetryPolicy = RetryPolicies.RetryExponential(
                RetryPolicies.DefaultClientRetryCount, RetryPolicies.DefaultClientBackoff);
            this.Timeout = (int)TimeSpan.FromSeconds(90).TotalSeconds;
        }

        #endregion

        #region Public Properties

        /// <summary>Gets or sets the retry policy requests made via the service context.</summary>
        /// <value>The retry policy. </value>
        public RetryPolicy RetryPolicy { get; set; }

        /// <summary>Gets the storage account credentials used by the service context.</summary>
        /// <value>The account credentials. </value>
        public StorageCredentials StorageCredentials { get; private set; }

        #endregion

        #region Public Methods and Operators

        /// <summary>Begins an asynchronous operation to save changes, using the retry policy specified for the service context.</summary>
        /// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes. </param>
        /// <param name="state">A user-defined object that will be passed to the callback delegate. </param>
        /// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation. </returns>
        public IAsyncResult BeginSaveChangesWithRetries(AsyncCallback callback, object state)
        {
            return this.BeginSaveChangesWithRetries(this.SaveChangesDefaultOptions, callback, state);
        }

        /// <summary>Begins an asynchronous operation to save changes, using the retry policy specified for the service context.</summary>
        /// <param name="options">Additional options for saving changes. </param>
        /// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes. </param>
        /// <param name="state">A user-defined object that will be passed to the callback delegate. </param>
        /// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation. </returns>
        public IAsyncResult BeginSaveChangesWithRetries(
            SaveChangesOptions options, AsyncCallback callback, object state)
        {
            return
                TaskImplHelper.BeginImplWithRetry<DataServiceResponse>(
                    setResult => this.SaveChangesWithRetriesImpl(options, setResult), 
                    this.RetryPolicy, 
                    callback, 
                    state);
        }

        /// <summary>Ends an asynchronous operation to save changes.</summary>
        /// <param name="asyncResult">An <see cref="IAsyncResult"/> that references the pending asynchronous operation. </param>
        /// <returns>A <see cref="DataServiceResponse"/> that represents the result of the operation. </returns>
        [SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", 
            Justification = "This is a member-operation")]
        public DataServiceResponse EndSaveChangesWithRetries(IAsyncResult asyncResult)
        {
            return TaskImplHelper.EndImplWithRetry<DataServiceResponse>(asyncResult);
        }

        /// <summary>Saves changes, using the retry policy specified for the service context.</summary>
        /// <returns>A <see cref="DataServiceResponse"/> that represents the result of the operation. </returns>
        public DataServiceResponse SaveChangesWithRetries()
        {
            return this.SaveChangesWithRetries(this.SaveChangesDefaultOptions);
        }

        /// <summary>Saves changes, using the retry policy specified for the service context.</summary>
        /// <param name="options">Additional options for saving changes. </param>
        /// <returns>A <see cref="DataServiceResponse"/> that represents the result of the operation. </returns>
        public DataServiceResponse SaveChangesWithRetries(SaveChangesOptions options)
        {
            return
                TaskImplHelper.ExecuteImplWithRetry<DataServiceResponse>(
                    setResult => this.SaveChangesWithRetriesImpl(options, setResult), this.RetryPolicy);
        }

        #endregion

        #region Methods

        /// <summary>Callback on DataContext object sending request.</summary>
        /// <param name="sender">The sender. </param>
        /// <param name="e">The <see cref="System.Data.Services.Client.SendingRequestEventArgs"/> instance containing the event data. </param>
        private void DataContextSendingRequest(object sender, SendingRequestEventArgs e)
        {
            var request = e.Request as HttpWebRequest;

            Debug.Assert(request != null, "request != null");
            request.Headers.Add(Constants.HeaderConstants.StorageVersionHeader, Request.GetTargetVersion());

            this.StorageCredentials.SignRequestLite(request);

            CommonUtils.ApplyRequestOptimizations(request, -1);
        }

        /// <summary>Saves the changes with retries implementation.</summary>
        /// <param name="options">The options for saving changes. </param>
        /// <param name="setResult">The action to set result. </param>
        /// <returns>A sequence of tasks to perform the operation. </returns>
        private TaskSequence SaveChangesWithRetriesImpl(
            SaveChangesOptions options, Action<DataServiceResponse> setResult)
        {
            var task = this.SaveChangesAsync(options);

            yield return task;

            setResult(task.Result);
        }

        #endregion
    }
}