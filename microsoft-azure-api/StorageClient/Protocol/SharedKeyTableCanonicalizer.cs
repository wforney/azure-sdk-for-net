﻿//-----------------------------------------------------------------------
// <copyright file="SharedKeyTableCanonicalizer.cs" company="Microsoft">
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
//    Contains code for the SharedKeyTableCanonicalizer class.
// </summary>
//-----------------------------------------------------------------------

namespace Microsoft.WindowsAzure.StorageClient.Protocol
{
    using System;
    using System.Globalization;
    using System.Net;

    /// <summary>
    /// Provides an implementation of the <see cref="CanonicalizationStrategy"/> class for requests against the Table service under the Shared Key authentication scheme.
    /// </summary>
    public sealed class SharedKeyTableCanonicalizer : CanonicalizationStrategy
    {
        #region Public Methods and Operators

        /// <summary>
        /// Canonicalizes the HTTP request.
        /// </summary>
        /// <param name="request">
        /// A web request. 
        /// </param>
        /// <param name="accountName">
        /// The name of the storage account. 
        /// </param>
        /// <returns>
        /// The canonicalized string for the request. 
        /// </returns>
        public override string CanonicalizeHttpRequest(HttpWebRequest request, string accountName)
        {
            var canonicalizedString = new CanonicalizedString(request.Method);

            var httpContentMD5Value = request.Headers[HttpRequestHeader.ContentMd5];

            canonicalizedString.AppendCanonicalizedElement(httpContentMD5Value);

            var contentType = request.ContentType;
            canonicalizedString.AppendCanonicalizedElement(contentType);

            var date = request.Headers[Constants.HeaderConstants.Date];
            if (string.IsNullOrEmpty(date))
            {
                var errorMessage = string.Format(CultureInfo.CurrentCulture, SR.MissingXmsDateInHeader);
                throw new ArgumentException(errorMessage, "request");
            }

            canonicalizedString.AppendCanonicalizedElement(date);

            canonicalizedString.AppendCanonicalizedElement(GetCanonicalizedResource(request.Address, accountName));

            return canonicalizedString.ToString();
        }

        #endregion
    }
}