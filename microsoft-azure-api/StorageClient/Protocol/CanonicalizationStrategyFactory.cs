//-----------------------------------------------------------------------
// <copyright file="CanonicalizationStrategyFactory.cs" company="Microsoft">
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
//    Contains code for the CanonicalizationStrategyFactory class.
// </summary>
//-----------------------------------------------------------------------

namespace Microsoft.WindowsAzure.StorageClient.Protocol
{
    using System;
    using System.Globalization;
    using System.Net;

    /// <summary>
    ///   Retrieve appropriate version of CanonicalizationStrategy based on the webrequest for Blob Queue and Table.
    /// </summary>
    internal static class CanonicalizationStrategyFactory
    {
        #region Constants and Fields

        /// <summary>
        ///   Stores the version 1 blob/queue full signing strategy.
        /// </summary>
        private static SharedKeyLiteCanonicalizer blobQueueFullVer1;

        /// <summary>
        ///   Stores the version 2 blob/queue full signing strategy.
        /// </summary>
        private static SharedKeyCanonicalizer blobQueueFullVer2;

        /// <summary>
        ///   Stores the version 1 table full signing strategy.
        /// </summary>
        private static SharedKeyTableCanonicalizer tableFullVer1;

        /// <summary>
        ///   Stores the version 1 table lite signing strategy.
        /// </summary>
        private static SharedKeyLiteTableCanonicalizer tableLiteVer1;

        #endregion

        #region Properties

        /// <summary>
        ///   Gets the BLOB queue full ver1.
        /// </summary>
        /// <value> The BLOB queue full ver1. </value>
        private static SharedKeyLiteCanonicalizer BlobQueueFullVer1
        {
            get
            {
                return blobQueueFullVer1 ?? (blobQueueFullVer1 = new SharedKeyLiteCanonicalizer());
            }
        }

        /// <summary>
        ///   Gets the BLOB queue full ver2.
        /// </summary>
        /// <value> The BLOB queue full ver2. </value>
        private static SharedKeyCanonicalizer BlobQueueFullVer2
        {
            get
            {
                return blobQueueFullVer2 ?? (blobQueueFullVer2 = new SharedKeyCanonicalizer());
            }
        }

        /// <summary>
        ///   Gets the table full ver1.
        /// </summary>
        /// <value> The table full ver1. </value>
        private static SharedKeyTableCanonicalizer TableFullVer1
        {
            get
            {
                return tableFullVer1 ?? (tableFullVer1 = new SharedKeyTableCanonicalizer());
            }
        }

        /// <summary>
        ///   Gets the table lite ver1.
        /// </summary>
        /// <value> The table lite ver1. </value>
        private static SharedKeyLiteTableCanonicalizer TableLiteVer1
        {
            get
            {
                return tableLiteVer1 ?? (tableLiteVer1 = new SharedKeyLiteTableCanonicalizer());
            }
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        ///   Gets canonicalization strategy for Blob and Queue SharedKey Authentication.
        /// </summary>
        /// <param name="request"> The request. </param>
        /// <returns> The canonicalization strategy. </returns>
        public static CanonicalizationStrategy GetBlobQueueFullCanonicalizationStrategy(HttpWebRequest request)
        {
            return IsTargetVersion2(request) ? (CanonicalizationStrategy)BlobQueueFullVer2 : BlobQueueFullVer1;
        }

        /// <summary>
        ///   Gets the BLOB queue lite canonicalization strategy.
        /// </summary>
        /// <param name="request"> The request. </param>
        /// <returns> The canonicalization strategy. </returns>
        public static CanonicalizationStrategy GetBlobQueueLiteCanonicalizationStrategy(HttpWebRequest request)
        {
            if (IsTargetVersion2(request))
            {
                // Old SharedKey Authentication is the new SharedKeyLite Authentication
                return BlobQueueFullVer1;
            }
            
            var errorMessage = string.Format(
                CultureInfo.CurrentCulture,
                SR.BlobQSharedKeyLiteUnsuppported,
                request.Headers[Constants.HeaderConstants.StorageVersionHeader]);
            throw new NotSupportedException(errorMessage);
        }

        /// <summary>
        ///   Gets the table full canonicalization strategy.
        /// </summary>
        /// <param name="request"> The request. </param>
        /// <returns> The canonicalization strategy. </returns>
        public static CanonicalizationStrategy GetTableFullCanonicalizationStrategy(HttpWebRequest request)
        {
            return TableFullVer1;
        }

        /// <summary>
        ///   Get canonicalization strategy for Tables for SharedKeyLite Authentication.
        /// </summary>
        /// <param name="request"> The request. </param>
        /// <returns> The canonicalization strategy. </returns>
        public static CanonicalizationStrategy GetTableLiteCanonicalizationStrategy(HttpWebRequest request)
        {
            return TableLiteVer1;
        }

        #endregion

        #region Methods

        /// <summary>
        ///   Determines whether [is target version2] [the specified request].
        /// </summary>
        /// <param name="request"> The request. </param>
        /// <returns> Returns <c>true</c> if [is target version2] [the specified request]; otherwise, <c>false</c> . </returns>
        private static bool IsTargetVersion2(HttpWebRequest request)
        {
            var version = request.Headers[Constants.HeaderConstants.StorageVersionHeader];
            DateTime versionTime;

            if (DateTime.TryParse(
                version, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal, out versionTime))
            {
                var canonicalizationVer2Date = new DateTime(2009, 09, 19);

                return versionTime.Date >= canonicalizationVer2Date;
            }

            return version.Equals("2009-09-19");
        }

        #endregion
    }
}