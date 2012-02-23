//-----------------------------------------------------------------------
// <copyright file="GetPageRangesResponse.cs" company="Microsoft">
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
//    Contains code for the GetPageRangesResponse class.
// </summary>
//-----------------------------------------------------------------------

namespace Microsoft.WindowsAzure.StorageClient.Protocol
{
    using System.Collections.Generic;
    using System.IO;
    using System.Xml;

    /// <summary>
    ///   Provides methods for parsing the response from an operation to get a range of pages for a page blob.
    /// </summary>
    public class GetPageRangesResponse : ResponseParsingBase<PageRange>
    {
        #region Constructors and Destructors

        /// <summary>
        ///   Initializes a new instance of the <see cref="GetPageRangesResponse" /> class.
        /// </summary>
        /// <param name="stream"> The stream of page ranges to be parsed. </param>
        internal GetPageRangesResponse(Stream stream)
            : base(stream)
        {
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Gets an enumerable collection of <see cref="PageRange" /> objects from the response.
        /// </summary>
        /// <value> An enumerable collection of <see cref="PageRange" /> objects. </value>
        public IEnumerable<PageRange> PageRanges
        {
            get
            {
                return this.ObjectsToParse;
            }
        }

        #endregion

        #region Methods

        /// <summary>
        ///   Parses the XML response for an operation to get a range of pages for a page blob.
        /// </summary>
        /// <returns> An enumerable collection of <see cref="PageRange" /> objects. </returns>
        protected override IEnumerable<PageRange> ParseXml()
        {
            // While we're still in the QueueMessageList section.
            while (this.Reader.Read())
            {
                // We found a queue message.
                if (this.Reader.NodeType == XmlNodeType.Element && !this.Reader.IsEmptyElement
                    && this.Reader.Name == Constants.PageRangeElement)
                {
                    PageRange pageRange = null;
                    var start = 0L;
                    var end = 0L;
                    var needToRead = true;

                    // Go until we are out of the block.
                    while (true)
                    {
                        if (needToRead && !this.Reader.Read())
                        {
                            break;
                        }

                        needToRead = true;

                        if (this.Reader.NodeType == XmlNodeType.Element && !this.Reader.IsEmptyElement)
                        {
                            switch (this.Reader.Name)
                            {
                                case Constants.StartElement:
                                    start = this.Reader.ReadElementContentAsLong();
                                    needToRead = false;
                                    break;
                                case Constants.EndElement:
                                    end = this.Reader.ReadElementContentAsLong();
                                    needToRead = false;
                                    break;
                            }
                        }
                        else if (this.Reader.NodeType == XmlNodeType.EndElement
                                 && this.Reader.Name == Constants.PageRangeElement)
                        {
                            pageRange = new PageRange(start, end);
                            break;
                        }
                    }

                    yield return pageRange;
                }
                else if (this.Reader.NodeType == XmlNodeType.EndElement && this.Reader.Name == Constants.PageListElement)
                {
                    break;
                }
            }
        }

        #endregion
    }
}