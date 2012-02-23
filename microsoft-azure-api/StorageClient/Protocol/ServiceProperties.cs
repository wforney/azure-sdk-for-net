//-----------------------------------------------------------------------
// <copyright file="ServiceProperties.cs" company="Microsoft">
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
//    Contains code for the ServiceProperties class.
// </summary>
//-----------------------------------------------------------------------

namespace Microsoft.WindowsAzure.StorageClient.Protocol
{
    using System;
    using System.Diagnostics;
    using System.Xml.Linq;

    /// <summary>
    /// Class representing a set of properties pertaining to a cloud storage service.
    /// </summary>
    public class ServiceProperties
    {
        #region Constants and Fields

        /// <summary>
        /// The name of the days XML element.
        /// </summary>
        internal const string DaysName = "Days";

        /// <summary>
        /// The name of the default service version XML element.
        /// </summary>
        internal const string DefaultServiceVersionName = "DefaultServiceVersion";

        /// <summary>
        /// The name of the delete operation XML element.
        /// </summary>
        internal const string DeleteName = "Delete";

        /// <summary>
        /// The name of the enabled XML element.
        /// </summary>
        internal const string EnabledName = "Enabled";

        /// <summary>
        /// The name of the include APIs XML element.
        /// </summary>
        internal const string IncludeApisName = "IncludeAPIs";

        /// <summary>
        /// The name of the logging XML element.
        /// </summary>
        internal const string LoggingName = "Logging";

        /// <summary>
        /// The name of the metrics XML element.
        /// </summary>
        internal const string MetricsName = "Metrics";

        /// <summary>
        /// The name of the read operation XML element.
        /// </summary>
        internal const string ReadName = "Read";

        /// <summary>
        /// The name of the retention policy XML element.
        /// </summary>
        internal const string RetentionPolicyName = "RetentionPolicy";

        /// <summary>
        /// The name of the root XML element.
        /// </summary>
        internal const string StorageServicePropertiesName = "StorageServiceProperties";

        /// <summary>
        /// The name of the version XML element.
        /// </summary>
        internal const string VersionName = "Version";

        /// <summary>
        /// The name of the write operation XML element.
        /// </summary>
        internal const string WriteName = "Write";

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the ServiceProperties class.
        /// </summary>
        public ServiceProperties()
        {
            this.Logging = new LoggingProperties();
            this.Metrics = new MetricsProperties();
        }

        #endregion

        #region Public Properties

        /// <summary>
        /// Gets or sets the default service version.
        /// </summary>
        /// <value>
        /// The default service version identifier. 
        /// </value>
        public string DefaultServiceVersion { get; set; }

        /// <summary>
        /// Gets or sets the logging properties.
        /// </summary>
        /// <value>
        /// The logging properties. 
        /// </value>
        public LoggingProperties Logging { get; set; }

        /// <summary>
        /// Gets or sets the metrics properties.
        /// </summary>
        /// <value>
        /// The metrics properties. 
        /// </value>
        public MetricsProperties Metrics { get; set; }

        #endregion

        #region Methods

        /// <summary>
        /// Constructs a <c>ServiceProperties</c> object from an XML document received from the service.
        /// </summary>
        /// <param name="servicePropertiesDocument">
        /// The XML document. 
        /// </param>
        /// <returns>
        /// A <c>ServiceProperties</c> object containing the properties in the XML document. 
        /// </returns>
        internal static ServiceProperties FromServiceXml(XDocument servicePropertiesDocument)
        {
            var servicePropertiesElement = servicePropertiesDocument.Element(StorageServicePropertiesName);
            Debug.Assert(servicePropertiesElement != null, "servicePropertiesElement != null");
            var properties = new ServiceProperties
                {
                    Logging = ReadLoggingPropertiesFromXml(servicePropertiesElement.Element(LoggingName)), 
                    Metrics = ReadMetricsPropertiesFromXml(servicePropertiesElement.Element(MetricsName))
                };

            var defaultServiceVersionXml = servicePropertiesElement.Element(DefaultServiceVersionName);
            if (defaultServiceVersionXml != null)
            {
                properties.DefaultServiceVersion = defaultServiceVersionXml.Value;
            }

            return properties;
        }

        /// <summary>
        /// Converts these properties into XML for communicating with the service.
        /// </summary>
        /// <returns>
        /// An XML document containing the service properties. 
        /// </returns>
        internal XDocument ToServiceXml()
        {
            if (this.Logging == null)
            {
                throw new InvalidOperationException("Logging cannot be null.");
            }

            if (this.Metrics == null)
            {
                throw new InvalidOperationException("Metrics cannot be null.");
            }

            var storageServiceElement = new XElement(
                StorageServicePropertiesName, GenerateLoggingXml(this.Logging), GenerateMetricsXml(this.Metrics));

            if (this.DefaultServiceVersion != null)
            {
                storageServiceElement.Add(new XElement(DefaultServiceVersionName, this.DefaultServiceVersion));
            }

            return new XDocument(storageServiceElement);
        }

        /// <summary>
        /// Generates XML representing the given logging properties.
        /// </summary>
        /// <param name="logging">
        /// The logging properties. 
        /// </param>
        /// <returns>
        /// An XML logging element. 
        /// </returns>
        private static XElement GenerateLoggingXml(LoggingProperties logging)
        {
            if ((LoggingOperations.All & logging.LoggingOperations) != logging.LoggingOperations)
            {
                throw new InvalidOperationException("Invalid logging operations specified.");
            }

            if (string.IsNullOrEmpty(logging.Version))
            {
                throw new InvalidOperationException("The logging version is null or empty.");
            }

            return new XElement(
                LoggingName, 
                new XElement(VersionName, logging.Version), 
                new XElement(DeleteName, (logging.LoggingOperations & LoggingOperations.Delete) != 0), 
                new XElement(ReadName, (logging.LoggingOperations & LoggingOperations.Read) != 0), 
                new XElement(WriteName, (logging.LoggingOperations & LoggingOperations.Write) != 0), 
                GenerateRetentionPolicyXml(logging.RetentionDays));
        }

        /// <summary>
        /// Generates XML representing the given metrics properties.
        /// </summary>
        /// <param name="metrics">
        /// The metrics properties. 
        /// </param>
        /// <returns>
        /// An XML metrics element. 
        /// </returns>
        private static XElement GenerateMetricsXml(MetricsProperties metrics)
        {
            if (!Enum.IsDefined(typeof(MetricsLevel), metrics.MetricsLevel))
            {
                throw new InvalidOperationException("Invalid metrics level specified.");
            }

            if (string.IsNullOrEmpty(metrics.Version))
            {
                throw new InvalidOperationException("The metrics version is null or empty.");
            }

            var enabled = metrics.MetricsLevel != MetricsLevel.None;

            var xml = new XElement(
                MetricsName, 
                new XElement(VersionName, metrics.Version), 
                new XElement(EnabledName, enabled), 
                GenerateRetentionPolicyXml(metrics.RetentionDays));

            if (enabled)
            {
                xml.Add(new XElement(IncludeApisName, metrics.MetricsLevel == MetricsLevel.ServiceAndApi));
            }

            return xml;
        }

        /// <summary>
        /// Generates XML representing the given retention policy.
        /// </summary>
        /// <param name="retentionDays">
        /// The number of days to retain, or null if the policy is disabled. 
        /// </param>
        /// <returns>
        /// An XML retention policy element. 
        /// </returns>
        private static XElement GenerateRetentionPolicyXml(int? retentionDays)
        {
            var enabled = retentionDays != null;
            var xml = new XElement(RetentionPolicyName, new XElement(EnabledName, enabled));

            if (enabled)
            {
                xml.Add(new XElement(DaysName, (int)retentionDays));
            }

            return xml;
        }

        /// <summary>
        /// Constructs a <c>LoggingProperties</c> object from an XML element.
        /// </summary>
        /// <param name="element">
        /// The XML element. 
        /// </param>
        /// <returns>
        /// A <c>LoggingProperties</c> object containing the properties in the element. 
        /// </returns>
        private static LoggingProperties ReadLoggingPropertiesFromXml(XElement element)
        {
            var state = LoggingOperations.None;

            Debug.Assert(element != null, "element != null");
            var xElement1 = element.Element(DeleteName);
            if (xElement1 != null && bool.Parse(xElement1.Value))
            {
                state |= LoggingOperations.Delete;
            }

            var xElement = element.Element(ReadName);
            if (xElement != null && bool.Parse(xElement.Value))
            {
                state |= LoggingOperations.Read;
            }

            var element1 = element.Element(WriteName);
            if (element1 != null && bool.Parse(element1.Value))
            {
                state |= LoggingOperations.Write;
            }

            return new LoggingProperties
                {
                    Version = element.Element(VersionName).ValueOrDefault(), 
                    LoggingOperations = state, 
                    RetentionDays = ReadRetentionPolicyFromXml(element.Element(RetentionPolicyName))
                };
        }

        /// <summary>
        /// Constructs a <c>MetricsProperties</c> object from an XML element.
        /// </summary>
        /// <param name="element">
        /// The XML element. 
        /// </param>
        /// <returns>
        /// A <c>MetricsProperties</c> object containing the properties in the element. 
        /// </returns>
        private static MetricsProperties ReadMetricsPropertiesFromXml(XElement element)
        {
            var state = MetricsLevel.None;

            if (bool.Parse(element.Element(EnabledName).ValueOrDefault()))
            {
                state = MetricsLevel.Service;

                if (bool.Parse(element.Element(IncludeApisName).ValueOrDefault()))
                {
                    state = MetricsLevel.ServiceAndApi;
                }
            }

            return new MetricsProperties
                {
                    Version = element.Element(VersionName).ValueOrDefault(), 
                    MetricsLevel = state, 
                    RetentionDays = ReadRetentionPolicyFromXml(element.Element(RetentionPolicyName))
                };
        }

        /// <summary>
        /// Constructs a retention policy (number of days) from an XML element.
        /// </summary>
        /// <param name="element">
        /// The XML element. 
        /// </param>
        /// <returns>
        /// The number of days to retain, or null if retention is disabled. 
        /// </returns>
        private static int? ReadRetentionPolicyFromXml(XElement element)
        {
            return !bool.Parse(element.Element(EnabledName).ValueOrDefault())
                       ? (int?)null
                       : int.Parse(element.Element(DaysName).ValueOrDefault());
        }

        #endregion
    }
}