namespace ServiceBusMessageForwarder.Logging
{
    public interface ILogger
    {
        void Log(string message, int indentationLevel = 0, int newLines = 0);
        void SetLogFile(string filename);
    }
}