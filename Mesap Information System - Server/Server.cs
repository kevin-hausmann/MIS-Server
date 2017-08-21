using M4DBO;
using System;
using System.Net;
using System.Threading;

namespace MesapInformationSystem
{
    /// <summary>
    /// Server for the MESAP Information System. Processes requests from clients.
    /// </summary>
    class Server
    {
        // MESAP client server system connection settings
        private const String TITLE = "MESAP Information System";
        private const String SHORT_TITLE = "MIS";
        private const bool USE_SYSTEM_DB = true;
        
        // Port we will listen on
        private const int PORT = 5050;

        // Stop command
        private const String STOP_COMMAND = "stop";

        // Listener
        private static HttpListener listener;

        // Root object for database access
        private static dboRoot root;

        // Global generators for response lists
        private static UserListGenerator userLister;
        private static ChangeListGenerator changeLister;

        /// <summary>
        /// Starts the main server thread. All requests are handled in seperate threads
        /// </summary>
        /// <param name="args">No arguments accepted</param>
        static void Main(string[] args)
        {
            // Create objects needed for MESAP Server access
            root = new M4DBO.dboRoot();

            mspErrDboInitEnum rootError = root.Initialize("", mspM4AppEnum.mspM4AppOEM, false, TITLE);
            if (rootError != mspErrDboInitEnum.mspErrNone)
            {
                WriteStatus("Can not connect to MESAP server.");
                WriteStatus("Stopped.");
                WriteStatus("Press any key to close...");
                Console.ReadKey();
                return;
            }

            // Login to MESAP Message Server
            mspErrDboLoginEnum loginError = root.Login(Private.User, Private.Password, USE_SYSTEM_DB, Private.SystemPassword);
            if (loginError != mspErrDboLoginEnum.mspErrNone)
            {
                WriteStatus("Can not log in as " + Private.User + "!");
                WriteStatus("Stopped.");
                WriteStatus("Press any key to close...");
                Console.ReadKey();
                return;
            }

            userLister = new UserListGenerator(root);
            changeLister = new ChangeListGenerator(root);

            // Start listing on the given port with given prefixes
            listener = new HttpListener();
            listener.Prefixes.Add("http://localhost:" + PORT + "/users/");
            //listener.Prefixes.Add("http://172.22.2.96:" + PORT + "/users/");
            listener.Prefixes.Add("http://localhost:" + PORT + "/changes/");
            //listener.Prefixes.Add("http://172.22.2.96:" + PORT + "/changes/");
            listener.Start();

            WriteStatus("Server up and running.");
            WriteStatus("Listening to port " + PORT + ".");
            WriteStatus("Type \"" + STOP_COMMAND + "\" to terminate the server.");

            // Start the thread which calls the method 'StartListen'
            Thread worker = new Thread(new ThreadStart(StartListen));
            worker.Start();

            // Read commands
            String cmd = "";
            while (!cmd.ToLower().Equals(STOP_COMMAND))
            {
                cmd = Console.ReadLine();
                if (!cmd.ToLower().Equals(STOP_COMMAND))
                    Console.WriteLine("Unknown command: " + cmd);
            }

            // Stop worker thread
            worker.Abort();
            
            // Stop listener
            listener.Stop();

            // Log out from MESAP system
            root.Logout();
        }

        private static void StartListen()
        {
            // Read installed users from database, should be done seldomly
            // so once per server session is sufficient (we might want to add something clever here later...)
            root.InstalledUsers.DbReadAll();
            
            while (true)
            {
                // Wait for connection
                HttpListenerContext context = listener.GetContext();
                WriteStatus("New client accepted: " + context.Request.RemoteEndPoint.ToString());

                // Start new server thread
                new ServerThread(context, userLister, changeLister);
            }
        }

        public static void WriteStatus(String message)
        {
            Console.WriteLine("[" + SHORT_TITLE + "] " + message);
        }
    }

    class ServerThread
    {
        // Handle on generators for response lists
        private UserListGenerator userLister;
        private ChangeListGenerator changeLister;

        // The JavaScript function name to prefix response with
        private const String JS_CALLBACK_FUNCTION_NAME_KEY = "callback";

        // Change list length parameter
        private const String HOURS_BACK_PARAMETER = "hours";

        // Chnage list include values parameter
        private const String INCLUDE_VALUES_PARAMETER = "values";

        // The client
        private HttpListenerContext context = null;

        // Create and start server thread
        public ServerThread(HttpListenerContext context, UserListGenerator userLister, ChangeListGenerator changeLister)
        {
            this.context = context;
       
            this.userLister = userLister;
            this.changeLister = changeLister;

            new Thread(new ThreadStart(Run)).Start();
        }

        // Respond to the actual request
        public void Run()
        {
            HttpListenerRequest request = context.Request;
            HttpListenerResponse response = context.Response;
            DateTime requestReceived = DateTime.Now;

            Server.WriteStatus("Request received: " + request.Url);
                       
            // Construct a response.
            String responseString;

            // Deviate depending on request
            if (request.Url.AbsolutePath.Contains("users")) responseString = userLister.Generate();
            else if(request.Url.AbsolutePath.Contains("changes")) 
                responseString = changeLister.Generate(request.QueryString.Get(HOURS_BACK_PARAMETER),
                        request.QueryString.Get(INCLUDE_VALUES_PARAMETER));
            else responseString = "Not implemented yet";

            // Finish and encode response
            responseString = request.QueryString.Get(JS_CALLBACK_FUNCTION_NAME_KEY) + "(" + responseString + ")";
            byte[] buffer = System.Text.Encoding.UTF8.GetBytes(responseString);
            
            // Get a response stream and write the response to it.
            response.ContentLength64 = buffer.Length;
            response.OutputStream.Write(buffer, 0, buffer.Length);
            
            // Close the output stream.
            response.OutputStream.Close();

            Server.WriteStatus("Response sent (" + buffer.Length + " bytes, " + (DateTime.Now - requestReceived).TotalSeconds + " seconds)");
        }        
    }
}
