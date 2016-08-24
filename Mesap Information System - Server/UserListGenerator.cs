using M4DBO;
using System;
using System.Collections;

namespace MesapInformationSystem
{
    /// <summary>
    /// Generates MESAP user list as a service for the MESAP Information System Server
    /// </summary>
    class UserListGenerator
    {
        // API access object
        private dboRoot root;

        // String constant to indicate that the user is offline
        private const String OFFLINE = "offline";

        internal UserListGenerator(dboRoot root)
        {
            this.root = root;
        }

        internal String Generate()
        {
            // Re-read login status
            root.Logins.ReadAll();

            String result = "[";
            
            // Put all users into result list
            IEnumerator users = root.InstalledUsers.GetEnumerator();
            while (users.MoveNext())
            {
                dboInstalledUser user = users.Current as dboInstalledUser;

                // skip ourselves
                if (user.ID.Equals(Private.User)) continue;

                result += "{\"name\": \"" + user.Name + "\", " +
                    "\"loggedIn\": \"" + IsUserLoggedIn(user.UserNr) + "\", " +
                    "\"databases\": " + ListDatabaseLoggedInto(user.UserNr) + ", " +
                    "\"lastSeenOnline\": \"" + user.LoginDate.ToString() + "\"},";
            }

            // Remove last comma and add missing bracket before return
            return result.Substring(0, result.Length - 1) + "]";
        }

        /// <summary>
        /// Checks whether a given user in currently logged in
        /// </summary>
        /// <param name="root">Database access object</param>
        /// <param name="userNr">The user number (identifier) for the user in question</param>
        /// <returns>A specific string constant to indicate that the user is offline or
        /// the exact time point at which the user did log in.</returns>
        private String IsUserLoggedIn(int userNr)
        {
            IEnumerator logins = root.Logins.GetEnumerator();
            while (logins.MoveNext())
            {
                dboLogin login = logins.Current as dboLogin;
                if (login.UserNr == userNr)
                    return login.LoginTime.ToString();
            }

            return OFFLINE;
        }

        /// <summary>
        /// List databases the user with given number is currently logged into.
        /// </summary>
        /// <param name="userNr">The user number (identifier) for the user in question</param>
        /// <returns>A JSON array string of the database names</returns>
        private String ListDatabaseLoggedInto(int userNr)
        {
            String result = "[";

            IEnumerator logins = root.Logins.GetEnumerator();
            while (logins.MoveNext())
            {
                dboLogin login = logins.Current as dboLogin;
                
                // Logged in, but no database open
                if (root.InstalledDbs[login.DbNr] == null) continue;
                // Find/Add name of database opened 
                else if (login.UserNr == userNr)
                    result += "\"" + root.InstalledDbs[login.DbNr].ID + "\",";
            }

            // Remove last comma and add missing bracket before return
            if (result.Length > 1) 
                result = result.Substring(0, result.Length - 1);

            return result + "]";
        }
    }
}
