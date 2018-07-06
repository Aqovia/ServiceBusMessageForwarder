using System;
using System.IO;

namespace ServiceBusMessageForwarder.Logging
{
    public class Logger : ILogger, IDisposable
    {
        private StreamWriter _streamWriter;
        private const string LogsDirectory = "Logs";
        private string _logFile;

        public Logger()
        {
            Directory.CreateDirectory(LogsDirectory);
        }

        public void SetLogFile(string filename)
        {
            _streamWriter?.Dispose();

            _logFile = filename;

            _streamWriter = File.AppendText($"{LogsDirectory}/{filename}");
        }

        public void Log(string message, int indentationLevel = 0, int newLines = 0)
        {
            if (_logFile == null)
                SetLogFile($"SBMF_LOG_{DateTime.UtcNow:yyyyMMdd}.log");

            var prefix = indentationLevel > 0
                ? new string('\t', indentationLevel) + ">> "
                : $"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}: ";

            var log = new string('\n', newLines) + prefix +  message;

            _streamWriter.WriteLine(log);
            Console.WriteLine(log);
        }

        public void Dispose()
        {
            _streamWriter?.Dispose();
        }
    }
}
