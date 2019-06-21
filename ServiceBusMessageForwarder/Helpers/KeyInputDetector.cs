using System;
using System.Threading;

namespace ServiceBusMessageForwarder.Helpers
{
    public class KeyInputDetector
    {
        private static readonly AutoResetEvent GetInput;
        private static readonly AutoResetEvent GotInput;

        static KeyInputDetector()
        {
            GetInput = new AutoResetEvent(false);
            GotInput = new AutoResetEvent(false);
            var inputThread = new Thread(Reader) { IsBackground = true };
            inputThread.Start();
        }

        private static void Reader()
        {
            while (true)
            {
                GetInput.WaitOne();
                Console.ReadKey();
                GotInput.Set();
            }
        }

        public static bool WaitForKey(TimeSpan timeout)
        {
            GetInput.Set();
            var success = GotInput.WaitOne(timeout);
            return success;
        }
    }
}
