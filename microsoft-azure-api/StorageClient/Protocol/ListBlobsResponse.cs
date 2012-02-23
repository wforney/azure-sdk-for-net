//-----------------------------------------------------------------------
// <copyright file="ListBlobsResponse.cs" company="Microsoft">
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
//    Contains code for the ListBlobsResponse class.
// </summary>
//-----------------------------------------------------------------------

namespace Microsoft.WindowsAzure.StorageClient.Protocol
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.IO;
    using System.Xml;

    /// <summary>
    ///   Provides methods for parsing the response from a blob listing operation.
    /// </summary>
    public class ListBlobsResponse : ResponseParsingBase<IListBlobEntry>
    {
        #region Constants and Fields

        /// <summary>
        ///   Stores the blob delimiter.
        /// </summary>
        private string delimiter;

        /// <summary>
        ///   Signals when the blob delimiter can be consumed.
        /// </summary>
        private bool delimiterConsumable;

        /// <summary>
        ///   Stores the marker.
        /// </summary>
        private string marker;

        /// <summary>
        ///   Signals when the marker can be consumed.
        /// </summary>
        private bool markerConsumable;

        /// <summary>
        ///   Stores the max results.
        /// </summary>
        private int maxResults;

        /// <summary>
        ///   Signals when the max results can be consumed.
        /// </summary>
        private bool maxResultsConsumable;

        /// <summary>
        ///   Stores the next marker.
        /// </summary>
        private string nextMarker;

        /// <summary>
        ///   Signals when the next marker can be consumed.
        /// </summary>
        private bool nextMarkerConsumable;

        /// <summary>
        ///   Stores the blob prefix.
        /// </summary>
        private string prefix;

        /// <summary>
        ///   Signals when the blob prefix can be consumed.
        /// </summary>
        private bool prefixConsumable;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        ///   Initializes a new instance of the <see cref="ListBlobsResponse" /> class.
        /// </summary>
        /// <param name="stream"> The stream to be parsed. </param>
        internal ListBlobsResponse(Stream stream)
            : base(stream)
        {
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Gets an enumerable collection of objects that implement <see cref="IListBlobEntry" /> from the response.
        /// </summary>
        /// <value> An enumerable collection of objects that implement <see cref="IListBlobEntry" /> . </value>
        public IEnumerable<IListBlobEntry> Blobs
        {
            get
            {
                return this.ObjectsToParse;
            }
        }

        /// <summary>
        ///   Gets the Delimiter value provided for the listing operation from the XML response.
        /// </summary>
        /// <value> The Delimiter value. </value>
        public string Delimiter
        {
            get
            {
                this.Variable(ref this.delimiterConsumable);

                return this.delimiter;
            }
        }

        /// <summary>
        ///   Gets the listing context from the XML response.
        /// </summary>
        /// <value> A set of parameters for the listing operation. </value>
        public BlobListingContext ListingContext
        {
            get
            {
                var p = this.Prefix;
                var mr = this.MaxResults;
                var d = this.Delimiter;
                var nm = this.NextMarker;
                var listingContext = new BlobListingContext(p, mr, d, BlobListingDetails.None)
                    { Marker = nm };
                return listingContext;
            }
        }

        /// <summary>
        ///   Gets the Marker value provided for the listing operation from the XML response.
        /// </summary>
        /// <value> The Marker value. </value>
        public string Marker
        {
            get
            {
                this.Variable(ref this.markerConsumable);

                return this.marker;
            }
        }

        /// <summary>
        ///   Gets the MaxResults value provided for the listing operation from the XML response.
        /// </summary>
        /// <value> The MaxResults value. </value>
        public int MaxResults
        {
            get
            {
                this.Variable(ref this.maxResultsConsumable);

                return this.maxResults;
            }
        }

        /// <summary>
        ///   Gets the NextMarker value from the XML response, if the listing was not complete.
        /// </summary>
        /// <value> The NextMarker value. </value>
        public string NextMarker
        {
            get
            {
                this.Variable(ref this.nextMarkerConsumable);

                return this.nextMarker;
            }
        }

        /// <summary>
        ///   Gets the Prefix value provided for the listing operation from the XML response.
        /// </summary>
        /// <value> The Prefix value. </value>
        public string Prefix
        {
            get
            {
                this.Variable(ref this.prefixConsumable);

                return this.prefix;
            }
        }

        #endregion

        #region Methods

        /// <summary>
        ///   Parses the response XML for a blob listing operation.
        /// </summary>
        /// <returns> An enumerable collection of objects that implement <see cref="IListBlobEntry" /> . </returns>
        protected override IEnumerable<IListBlobEntry> ParseXml()
        {
            var needToRead = true;

            while (true)
            {
                if (needToRead && !this.Reader.Read())
                {
                    break;
                }

                needToRead = true;

                // Run through the stream until we find what we are looking for.  Retain what we've found.
                if (this.Reader.NodeType == XmlNodeType.Element && !this.Reader.IsEmptyElement)
                {
                    switch (this.Reader.Name)
                    {
                        case Constants.DelimiterElement:
                            needToRead = false;
                            this.delimiter = this.Reader.ReadElementContentAsString();
                            this.delimiterConsumable = true;
                            yield return null;
                            break;
                        case Constants.MarkerElement:
                            needToRead = false;
                            this.marker = this.Reader.ReadElementContentAsString();
                            this.markerConsumable = true;
                            yield return null;
                            break;
                        case Constants.NextMarkerElement:
                            needToRead = false;
                            this.nextMarker = this.Reader.ReadElementContentAsString();
                            this.nextMarkerConsumable = true;
                            yield return null;
                            break;
                        case Constants.MaxResultsElement:
                            needToRead = false;
                            this.maxResults = this.Reader.ReadElementContentAsInt();
                            this.maxResultsConsumable = true;
                            yield return null;
                            break;
                        case Constants.PrefixElement:
                            needToRead = false;
                            this.prefix = this.Reader.ReadElementContentAsString();
                            this.prefixConsumable = true;
                            yield return null;
                            break;
                        case Constants.BlobsElement:
                            // While we're still in the blobs section.
                            while (this.Reader.Read())
                            {
                                // We found a blob.
                                if (this.Reader.NodeType == XmlNodeType.Element && !this.Reader.IsEmptyElement
                                    && this.Reader.Name == Constants.BlobElement)
                                {
                                    BlobAttributes blob = null;
                                    string url = null;
                                    DateTime? lastModifiedTime = null;
                                    string etag = null;
                                    string name = null;
                                    long? contentLength = null;
                                    string contentType = null;
                                    string contentEncoding = null;
                                    string contentLanguage = null;
                                    string contentMD5 = null;
                                    BlobType? blobType = null;
                                    LeaseStatus? leaseStatus = null;
                                    DateTime? snapshot = null;
                                    NameValueCollection metadata = null;

                                    // Go until we are out of the blob.
                                    var blobNeedToRead = true;

                                    while (true)
                                    {
                                        if (blobNeedToRead && !this.Reader.Read())
                                        {
                                            break;
                                        }

                                        blobNeedToRead = true;

                                        if (this.Reader.NodeType == XmlNodeType.Element && !this.Reader.IsEmptyElement)
                                        {
                                            switch (this.Reader.Name)
                                            {
                                                case Constants.UrlElement:
                                                    url = this.Reader.ReadElementContentAsString();
                                                    blobNeedToRead = false;
                                                    break;
                                                case Constants.LastModifiedElement:
                                                    lastModifiedTime =
                                                        this.Reader.ReadElementContentAsString().ToUTCTime();
                                                    blobNeedToRead = false;
                                                    break;
                                                case Constants.EtagElement:
                                                    etag = this.Reader.ReadElementContentAsString();
                                                    blobNeedToRead = false;
                                                    break;
                                                case Constants.NameElement:
                                                    name = this.Reader.ReadElementContentAsString();
                                                    blobNeedToRead = false;
                                                    break;
                                                case Constants.ContentLengthElement:
                                                    contentLength = this.Reader.ReadElementContentAsLong();
                                                    blobNeedToRead = false;
                                                    break;
                                                case Constants.ContentTypeElement:
                                                    contentType = this.Reader.ReadElementContentAsString();
                                                    blobNeedToRead = false;
                                                    break;
                                                case Constants.ContentEncodingElement:
                                                    contentEncoding = this.Reader.ReadElementContentAsString();
                                                    blobNeedToRead = false;
                                                    break;
                                                case Constants.ContentLanguageElement:
                                                    contentLanguage = this.Reader.ReadElementContentAsString();
                                                    blobNeedToRead = false;
                                                    break;
                                                case Constants.ContentMD5Element:
                                                    contentMD5 = this.Reader.ReadElementContentAsString();
                                                    blobNeedToRead = false;
                                                    break;
                                                case Constants.BlobTypeElement:
                                                    var blobTypeString = this.Reader.ReadElementContentAsString();
                                                    blobNeedToRead = false;

                                                    switch (blobTypeString)
                                                    {
                                                        case Constants.BlockBlobValue:
                                                            blobType = BlobType.BlockBlob;
                                                            break;
                                                        case Constants.PageBlobValue:
                                                            blobType = BlobType.PageBlob;
                                                            break;
                                                    }

                                                    break;
                                                case Constants.LeaseStatusElement:
                                                    var leaseStatusString = this.Reader.ReadElementContentAsString();
                                                    blobNeedToRead = false;

                                                    switch (leaseStatusString)
                                                    {
                                                        case Constants.LockedValue:
                                                            leaseStatus = LeaseStatus.Locked;
                                                            break;
                                                        case Constants.UnlockedValue:
                                                            leaseStatus = LeaseStatus.Unlocked;
                                                            break;
                                                    }

                                                    break;
                                                case Constants.SnapshotElement:
                                                    snapshot = this.Reader.ReadElementContentAsString().ToUTCTime();
                                                    blobNeedToRead = false;
                                                    break;
                                                case Constants.MetadataElement:
                                                    metadata = Response.ParseMetadata(this.Reader);
                                                    blobNeedToRead = false;
                                                    break;
                                            }
                                        }
                                        else if (this.Reader.NodeType == XmlNodeType.EndElement
                                                 && this.Reader.Name == Constants.BlobElement)
                                        {
                                            blob = new BlobAttributes
                                                {
                                                    Properties =
                                                        new BlobProperties
                                                            {
                                                                ContentEncoding = contentEncoding,
                                                                ContentLanguage = contentLanguage,
                                                                ContentMD5 = contentMD5,
                                                                Length = contentLength ?? 0,
                                                                ContentType = contentType,
                                                                ETag = etag
                                                            }
                                                };

                                            if (lastModifiedTime != null)
                                            {
                                                blob.Properties.LastModifiedUtc = (DateTime)lastModifiedTime;
                                            }

                                            var blobNameSectionIndex = url.LastIndexOf(NavigationHelper.Slash + name, StringComparison.Ordinal);
                                            var baseUri = url.Substring(0, blobNameSectionIndex + 1);
                                            var ub = new UriBuilder(baseUri);
                                            ub.Path += Uri.EscapeUriString(name);
                                            if (baseUri.Length + name.Length < url.Length)
                                            {
                                                // it's a url for snapshot. 
                                                // Snapshot blob URI example:http://<yourstorageaccount>.blob.core.windows.net/<yourcontainer>/<yourblobname>?snapshot=2009-12-03T15%3a26%3a19.4466877Z 
                                                ub.Query = url.Substring(baseUri.Length + name.Length + 1);
                                            }

                                            blob.Uri = ub.Uri;

                                            blob.Properties.LeaseStatus = leaseStatus ?? LeaseStatus.Unspecified;

                                            if (snapshot != null)
                                            {
                                                blob.Snapshot = snapshot;
                                            }

                                            blob.Properties.BlobType = blobType ?? BlobType.Unspecified;

                                            if (metadata != null)
                                            {
                                                blob.Metadata = metadata;
                                            }

                                            break;
                                        }
                                    }

                                    yield return new BlobEntry(name, blob);
                                }
                                else if (this.Reader.NodeType == XmlNodeType.Element && !this.Reader.IsEmptyElement
                                         && this.Reader.Name == Constants.BlobPrefixElement)
                                {
                                    var commonPrefix = new BlobPrefixEntry();

                                    // Go until we are out of the blob.
                                    var blobPrefixNeedToRead = true;

                                    while (true)
                                    {
                                        if (blobPrefixNeedToRead && !this.Reader.Read())
                                        {
                                            break;
                                        }

                                        blobPrefixNeedToRead = true;

                                        if (this.Reader.NodeType == XmlNodeType.Element
                                            && !this.Reader.IsEmptyElement)
                                        {
                                            switch (this.Reader.Name)
                                            {
                                                case Constants.NameElement:
                                                    commonPrefix.Name = this.Reader.ReadElementContentAsString();
                                                    blobPrefixNeedToRead = false;
                                                    break;
                                            }
                                        }
                                        else if (this.Reader.NodeType == XmlNodeType.EndElement
                                                 && this.Reader.Name == Constants.BlobPrefixElement)
                                        {
                                            break;
                                        }
                                    }

                                    yield return commonPrefix;
                                }
                                else if (this.Reader.NodeType == XmlNodeType.EndElement
                                         && this.Reader.Name == Constants.BlobsElement)
                                {
                                    this.AllObjectsParsed = true;
                                    break;
                                }
                            }

                            break;
                    }
                }
            }
        }

        #endregion
    }
}