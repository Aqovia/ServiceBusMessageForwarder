using ServiceBusMessageForwarder.Logging;

namespace ServiceBusMessageForwarder.IntegrationTests.Logging
{
    public class MockLogger : ILogger
    {
        public void Log(string message, int indentationLevel = 0, int newLines = 0) {}
        public void SetLogFile(string filename) {}
    }
}
