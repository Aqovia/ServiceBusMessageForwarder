using System.IO;
using System.Text.RegularExpressions;
using Microsoft.ServiceBus.Messaging;

namespace ServiceBusMessageForwarder.Extensions
{
    public static class MessageExtensions
    {
        public static string GetSingleLineContent(this BrokeredMessage message) => 
            Regex.Replace(new StreamReader(message.GetBody<Stream>()).ReadToEnd(), @"\r\n?|\n", "");
    }
}
