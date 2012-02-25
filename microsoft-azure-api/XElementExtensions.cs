// -----------------------------------------------------------------------
// <copyright file="XElementExtensions.cs" company="improvGroup, LLC">
// </copyright>
// -----------------------------------------------------------------------

namespace Microsoft.WindowsAzure
{
    using System.Xml.Linq;

    /// <summary>
    /// The XElement extensions.
    /// </summary>
    public static class XElementExtensions
    {
        /// <summary>
        /// Values the or default.
        /// </summary>
        /// <param name="xml">The XML.</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string ValueOrDefault(this XElement xml)
        {
            return xml == null ? null : xml.Value;
        }

        /// <summary>
        /// Values the or default.
        /// </summary>
        /// <param name="xml">The XML.</param>
        /// <param name="defaultValue">The default value.</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string ValueOrDefault(this XElement xml, string defaultValue)
        {
            return xml == null ? defaultValue : xml.Value;
        }
    }
}
