//-----------------------------------------------------------------------
// <copyright file="AccessPolicyResponse.cs" company="Microsoft">
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
//    Contains code for the AccessPolicyResponse class.
// </summary>
//-----------------------------------------------------------------------

namespace Microsoft.WindowsAzure.StorageClient.Protocol
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Xml;

    /// <summary>
    ///   Parses the response XML from an operation to set the access policy for a container.
    /// </summary>
    public class AccessPolicyResponse : ResponseParsingBase<KeyValuePair<string, SharedAccessPolicy>>
    {
        #region Constructors and Destructors

        /// <summary>
        ///   Initializes a new instance of the <see cref="AccessPolicyResponse" /> class.
        /// </summary>
        /// <param name="stream"> The stream to be parsed. </param>
        internal AccessPolicyResponse(Stream stream)
            : base(stream)
        {
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Gets an enumerable collection of container-level access policy identifiers.
        /// </summary>
        /// <value> An enumerable collection of container-level access policy identifiers. </value>
        public IEnumerable<KeyValuePair<string, SharedAccessPolicy>> AccessIdentifiers
        {
            get
            {
                return this.ObjectsToParse;
            }
        }

        #endregion

        #region Methods

        /// <summary>
        ///   Parses the response XML from a Set Container ACL operation to retrieve container-level access policy data.
        /// </summary>
        /// <returns> A list of enumerable key-value pairs. </returns>
        protected override IEnumerable<KeyValuePair<string, SharedAccessPolicy>> ParseXml()
        {
            var needToRead = true;
            while (true)
            {
                if (needToRead && !this.Reader.Read())
                {
                    break;
                }

                needToRead = true;

                if (this.Reader.NodeType != XmlNodeType.Element || this.Reader.IsEmptyElement
                    || this.Reader.Name != Constants.SignedIdentifiers)
                {
                    continue;
                }

                while (this.Reader.Read())
                {
                    if (this.Reader.NodeType != XmlNodeType.Element || this.Reader.IsEmptyElement
                        || this.Reader.Name != Constants.SignedIdentifier)
                    {
                        continue;
                    }
                    
                    while (this.Reader.Read())
                    {
                        if (this.Reader.NodeType != XmlNodeType.Element || this.Reader.IsEmptyElement
                            || this.Reader.Name != Constants.Id)
                        {
                            continue;
                        }
                        
                        var id = this.Reader.ReadElementContentAsString();
                        var identifier = new SharedAccessPolicy();
                        var needToReadItem = true;

                        do
                        {
                            if (this.Reader.NodeType != XmlNodeType.Element || this.Reader.IsEmptyElement
                                || this.Reader.Name != Constants.AccessPolicy)
                            {
                                continue;
                            }
                            
                            var needToReadElement = true;

                            while (true)
                            {
                                if (needToReadElement && !this.Reader.Read())
                                {
                                    break;
                                }

                                needToReadElement = true;

                                if (this.Reader.NodeType == XmlNodeType.Element
                                    && !this.Reader.IsEmptyElement)
                                {
                                    switch (this.Reader.Name)
                                    {
                                        case Constants.Start:
                                            identifier.SharedAccessStartTime =
                                                Uri.UnescapeDataString(
                                                    this.Reader.ReadElementContentAsString()).ToUTCTime(
                                                    );
                                            needToReadElement = false;
                                            break;
                                        case Constants.Expiry:
                                            identifier.SharedAccessExpiryTime =
                                                Uri.UnescapeDataString(
                                                    this.Reader.ReadElementContentAsString()).ToUTCTime(
                                                    );
                                            needToReadElement = false;
                                            break;
                                        case Constants.Permission:
                                            identifier.Permissions =
                                                SharedAccessPolicy.PermissionsFromString(
                                                    this.Reader.ReadElementContentAsString());
                                            needToReadElement = false;
                                            break;
                                    }
                                }
                                else if (this.Reader.NodeType == XmlNodeType.EndElement
                                         && this.Reader.Name == Constants.AccessPolicy)
                                {
                                    needToReadItem = false;
                                    break;
                                }
                            }
                        }
                        while (needToReadItem && this.Reader.Read());

                        yield return new KeyValuePair<string, SharedAccessPolicy>(id, identifier);
                    }
                }
            }
        }

        #endregion
    }
}