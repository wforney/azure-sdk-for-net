//-----------------------------------------------------------------------
// <copyright file="PeekMessagesResponse.cs" company="Microsoft">
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
//    Contains code for the PeekMessagesResponse class.
// </summary>
//-----------------------------------------------------------------------

namespace Microsoft.WindowsAzure.StorageClient.Protocol
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Xml;

    /// <summary>
    ///   Provides methods for parsing the response from an operation to peek messages from a queue.
    /// </summary>
    public class PeekMessagesResponse : ResponseParsingBase<QueueMessage>
    {
        #region Constructors and Destructors

        /// <summary>
        ///   Initializes a new instance of the <see cref="PeekMessagesResponse" /> class.
        /// </summary>
        /// <param name="stream"> The stream to be parsed. </param>
        internal PeekMessagesResponse(Stream stream)
            : base(stream)
        {
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Gets an enumerable collection of <see cref="QueueMessage" /> objects from the response.
        /// </summary>
        /// <value> An enumerable collection of <see cref="QueueMessage" /> objects. </value>
        public IEnumerable<QueueMessage> Messages
        {
            get
            {
                return this.ObjectsToParse;
            }
        }

        #endregion

        #region Methods

        /// <summary>
        ///   Parses the XML response returned by an operation to get messages from a queue.
        /// </summary>
        /// <returns> An enumerable collection of <see cref="QueueMessage" /> objects. </returns>
        protected override IEnumerable<QueueMessage> ParseXml()
        {
            // While we're still in the QueueMessageList section.
            while (this.Reader.Read())
            {
                // We found a queue message.
                if (this.Reader.NodeType == XmlNodeType.Element && !this.Reader.IsEmptyElement
                    && this.Reader.Name == Constants.MessageElement)
                {
                    QueueMessage message = null;
                    string id = null;
                    DateTime? insertionTime = null;
                    DateTime? expirationTime = null;
                    string text = null;
                    var dequeueCount = 0;

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
                                case Constants.MessageIdElement:
                                    id = this.Reader.ReadElementContentAsString();
                                    needToRead = false;
                                    break;
                                case Constants.InsertionTimeElement:
                                    insertionTime = this.Reader.ReadElementContentAsString().ToUTCTime();
                                    needToRead = false;
                                    break;
                                case Constants.ExpirationTimeElement:
                                    expirationTime = this.Reader.ReadElementContentAsString().ToUTCTime();
                                    needToRead = false;
                                    break;
                                case Constants.MessageTextElement:
                                    text = this.Reader.ReadElementContentAsString();
                                    needToRead = false;
                                    break;
                                case Constants.DequeueCountElement:
                                    dequeueCount = this.Reader.ReadElementContentAsInt();
                                    needToRead = false;
                                    break;
                            }
                        }
                        else if (this.Reader.NodeType == XmlNodeType.EndElement
                                 && this.Reader.Name == Constants.MessageElement)
                        {
                            message = new QueueMessage { Id = id, Text = text, DequeueCount = dequeueCount };

                            if (expirationTime != null)
                            {
                                message.ExpirationTime = (DateTime)expirationTime;
                            }

                            if (insertionTime != null)
                            {
                                message.InsertionTime = (DateTime)insertionTime;
                            }

                            break;
                        }
                    }

                    yield return message;
                }
                else if (this.Reader.NodeType == XmlNodeType.EndElement && this.Reader.Name == Constants.MessagesElement)
                {
                    break;
                }
            }
        }

        #endregion
    }
}