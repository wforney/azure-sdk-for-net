//-----------------------------------------------------------------------
// <copyright file="GetBlockListResponse.cs" company="Microsoft">
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
//    Contains code for the GetBlockListResponse class.
// </summary>
//-----------------------------------------------------------------------

namespace Microsoft.WindowsAzure.StorageClient.Protocol
{
    using System.Collections.Generic;
    using System.IO;
    using System.Xml;

    /// <summary>
    ///   Provides methods for parsing the response from an operation to return a block list.
    /// </summary>
    public class GetBlockListResponse : ResponseParsingBase<ListBlockItem>
    {
        #region Constructors and Destructors

        /// <summary>
        ///   Initializes a new instance of the <see cref="GetBlockListResponse" /> class.
        /// </summary>
        /// <param name="stream"> The stream to be parsed. </param>
        internal GetBlockListResponse(Stream stream)
            : base(stream)
        {
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Gets an enumerable collection of <see cref="ListBlockItem" /> objects from the response.
        /// </summary>
        /// <value> An enumerable collection of <see cref="ListBlockItem" /> objects. </value>
        public IEnumerable<ListBlockItem> Blocks
        {
            get
            {
                return this.ObjectsToParse;
            }
        }

        #endregion

        #region Methods

        /// <summary>
        ///   Parses the XML response returned by an operation to retrieve a list of blocks.
        /// </summary>
        /// <returns> An enumerable collection of <see cref="ListBlockItem" /> objects. </returns>
        protected override IEnumerable<ListBlockItem> ParseXml()
        {
            var committedBlocks = true;
            while (this.Reader.Read())
            {
                // Run through the stream until we find what we are looking for.  Retain what we've found.
                if (this.Reader.NodeType == XmlNodeType.Element && !this.Reader.IsEmptyElement)
                {
                    switch (this.Reader.Name)
                    {
                        case Constants.BlockElement:
                            // We found a block.
                            ListBlockItem block = null;
                            long size = 0;
                            string blockId = null;

                            // Go until we are out of the block.
                            var needToRead = true;
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
                                        case Constants.SizeElement:
                                            size = this.Reader.ReadElementContentAsLong();
                                            needToRead = false;
                                            break;
                                        case Constants.NameElement:
                                            blockId = this.Reader.ReadElementContentAsString();
                                            needToRead = false;
                                            break;
                                    }
                                }
                                else if (this.Reader.NodeType == XmlNodeType.EndElement
                                         && this.Reader.Name == Constants.BlockElement)
                                {
                                    block = new ListBlockItem
                                        { Name = blockId, Size = size, Committed = committedBlocks };
                                    break;
                                }
                            }

                            yield return block;
                            break;
                        case Constants.CommittedBlocksElement:
                            committedBlocks = true;
                            break;
                        case Constants.UncommittedBlocksElement:
                            committedBlocks = false;
                            break;
                    }
                }
            }
        }

        #endregion
    }
}