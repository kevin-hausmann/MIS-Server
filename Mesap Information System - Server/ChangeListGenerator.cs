using M4DBO;
using System;
using System.Collections;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;

namespace MesapInformationSystem
{
    /// <summary>
    /// Generates database changes list as a service for the MESAP Information System Server
    /// </summary>
    class ChangeListGenerator
    {
        // API access object
        private dboRoot root;

        // Cached result string containing all changes in JSON
        private String result;

        // Helper to memorize last hours back value
        private int lastHoursBack;

        // Helper to memorize include values
        private bool includeValues;

        // Datetime of last result generation
        private DateTime generatedOn;

        // Direct non-API database access
        private SqlConnection databaseConnection;

        // Life time of cache results
        private const int CACHE_LIFETIME_MINUTES = 10;

        // Default number of hours back in time to report changes for
        private const int DEFAULT_CHANGE_HOURS = 24;

        // Default include values
        private const bool DEFAULT_INCLUDE_VALUES = false;

        // String constants for change types
        private const String REPORT = "REPORT";
        private const String CALCULATION = "CALCULATION";
        private const String TREE = "TREE";
        private const String DESCRIPTOR = "DESCRIPTOR";
        private const String SERIES = "SERIES";
        private const String VALUE = "VALUE";
        private const String VIEW = "VIEW";
        private const String CRF = "CRF";

        private String[] databases = { "ESz", "BEU", "ZSE_aktuell", "PoSo", "ZSE_Submission_2012_20120305", "ZSE_Submission_2013_20130220" };

        private NameValueCollection userNameCache = new NameValueCollection();

        internal ChangeListGenerator(dboRoot root)
        {
            this.root = root;
        }

        internal String Generate(String hoursBackParameter, String includeValuesParameter)
        {
            // Find requested length of change list (how many hours back?)
            int hoursBack = DecodeHoursBack(hoursBackParameter);

            // Can we return cached result?
            if (result != null && !IsCacheExpired() && hoursBack.Equals(lastHoursBack)
                && includeValues.Equals(DecodeIncludeValues(includeValuesParameter))) return result;

            // Include values on this request?
            includeValues = DecodeIncludeValues(includeValuesParameter);

            // Store hours back value to decide on cache validity
            lastHoursBack = hoursBack;

            // Start generating result
            result = "[";

            // Cycle through all databases
            for (int db = 0; db < databases.Length; db++) 
                AppendChanges(databases[db], hoursBack);
            
            // If any changes happend, remove last comma
            if (result.Length > 1)
                result = result.Substring(0, result.Length - 1);

            // Add missing bracket before return
            result += "]";

            // Set cache stamp
            generatedOn = DateTime.Now;

            return result;
        }

        private void AppendChanges(String databaseId, int hoursBack)
        {
            try
            {
                Connect(databaseId);
                databaseConnection.Open();

                // Reports
                ProcessType(databaseId, "Report", REPORT, 2, 1, 14, 13, hoursBack);

                // Calculations
                ProcessType(databaseId, "CalculationMethod", CALCULATION, 2, 1, 11, 10, hoursBack);

                // Trees
                ProcessType(databaseId, "Tree", TREE, 2, 1, 8, 7, hoursBack);

                // Descriptors
                ProcessType(databaseId, "TreeObject", DESCRIPTOR, 2, 1, 12, 11, hoursBack);

                // TimeSeries
                ProcessType(databaseId, "TimeSeries", SERIES, 3, 2, 13, 11, hoursBack);

                // Views
                ProcessType(databaseId, "TimeSeriesView", VIEW, 2, 1, 16, 15, hoursBack);

                // CRF Variables
                ProcessType(databaseId, "CrfVariable", CRF, 2, 4, 14, 13, hoursBack);

                // Values
                if (includeValues) ProcessValues(databaseId, hoursBack);
            }
            catch (Exception ex)
            {
                Console.WriteLine("OOPS: " + ex.Message); // + ex.StackTrace);
            }
            finally
            {
                databaseConnection.Close();
            }
        }

        /// <summary>
        /// Generates history for a certain type of object
        /// </summary>
        /// <param name="databaseId">Name of database</param>
        /// <param name="table">Name of table objects are stored in</param>
        /// <param name="type">String to represent type in view</param>
        /// <param name="nameCol">Table column to read object's name from</param>
        /// <param name="idCol">Table column to read object's id from</param>
        /// <param name="dateCol">Table column to read last change date from</param>
        /// <param name="userCol">Table column to read user name from</param>
        private void ProcessType(String databaseId, String table, String type, 
            int nameCol, int idCol, int dateCol, int userCol, int hoursBack)
        {
            SqlDataReader reader = null;

            try
            {
                String query = "SELECT * FROM " + table + " WHERE (ChangeDate > '" + DateTime.Now.AddHours(-hoursBack) + "')";
                reader = new SqlCommand(query, databaseConnection).ExecuteReader();

                while (reader.Read())
                    result += "{\"database\": \"" + databaseId + "\", " +
                    "\"type\": \"" + type + "\", " +
                    "\"name\": \"" + reader.GetString(nameCol) + "\", " +
                    "\"id\": \"" + reader.GetString(idCol) + "\", " +
                    "\"user\": \"" + GetUserName(reader.GetString(userCol)) + "\", " +
                    "\"datetime\": \"" + reader.GetDateTime(dateCol) + "\"},";
            }
            catch (Exception ex)
            {
                // Do not show obvious exceptions
                if (!ex.Message.Contains("Crf")) Console.WriteLine("OOPS: " + ex.Message + ex.StackTrace);
            }
            finally
            {
                if (reader != null)
                    reader.Close();
            }
        }

        private void ProcessValues(String databaseId, int hoursBack)
        {
            SqlDataReader reader = null;

            try
            {
                String query = "SELECT TimeSeriesData.PeriodNr, TimeSeries.Id, TimeSeries.Name, TimeSeriesData.ChangeName, " +
                    "TimeSeriesData.ChangeDate FROM TimeSeriesData INNER JOIN TimeSeries " +
                    "ON TimeSeriesData.TsNr = TimeSeries.TsNr WHERE (TimeSeriesData.ChangeDate > '" + DateTime.Now.AddHours(-hoursBack) + "')";
                reader = new SqlCommand(query, databaseConnection).ExecuteReader();

                while (reader.Read())
                {
                    result += "{\"database\": \"" + databaseId + "\", " +
                        "\"type\": \"" + VALUE + " " + (reader.GetInt32(0) + 2000) + "\", " +
                        "\"name\": \"" + reader.GetString(2) + "\", " +
                        "\"id\": \"" + reader.GetString(1) + "\", " +
                        "\"user\": \"" + GetUserName(reader.GetString(3)) + "\", " +
                        "\"datetime\": \"" + reader.GetDateTime(4) + "\"},";
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("OOPS: " + ex.Message + ex.StackTrace);
            }
            finally
            {
                if (reader != null)
                    reader.Close();
            }
        }

        private String GetUserName(String userId)
        {
            if (userNameCache.Get(userId) != null && userNameCache.Get(userId).Length > 0) 
                return userNameCache.Get(userId);

            IEnumerator users = root.InstalledUsers.GetEnumerator();
            while (users.MoveNext())
            {
                dboInstalledUser user = users.Current as dboInstalledUser;
                if (user.ID.Equals(userId))
                {
                    userNameCache.Add(userId, user.Name);
                    return user.Name;
                }
            }

            return userId;
        }

        private bool IsCacheExpired()
        {
            return (DateTime.Now - generatedOn).TotalMinutes > CACHE_LIFETIME_MINUTES;
        }

        private int DecodeHoursBack(String hoursBackParameter)
        {
            int result;

            if (Int32.TryParse(hoursBackParameter, out result)) return result;
            else return DEFAULT_CHANGE_HOURS;
        }

        private bool DecodeIncludeValues(String includeValuesParameter)
        {
            bool result;

            if (Boolean.TryParse(includeValuesParameter, out result)) return result;
            else return DEFAULT_INCLUDE_VALUES;
        }

        private bool Connect(String databaseId)
        {
            if (databaseConnection != null && databaseConnection.State != ConnectionState.Closed)
                databaseConnection.Close();

            databaseConnection = new SqlConnection(BuildDBConnectionString(databaseId));

            return true;
        }

        private String BuildDBConnectionString(String databaseId)
        {
            String databaseName;
            switch (databaseId)
            {
                case "ESz":
                case "BEU":
                case "PoSo":
                case "Enerdat":
                    databaseName = databaseId.ToUpper(); break; 
                case "ZSE_aktuell":
                    databaseName = databaseId; break;
                case "ZSE_Submission_2016_20160203": 
                case "ZSE_Submission_2015_20150428":
                case "ZSE_Submission_2014_20140303":
                case "ZSE_Submission_2013_20130220":
                case "ZSE_Submission_2012_20120305":
                case "ZSE_Submission_2011_20110223":
                case "ZSE_Submission_2010_20100215":
                case "ZSE_Submission_2009_20090211":
                case "ZSE_Submission_2008_20080213":
                case "ZSE_Submission_2007_20070328":
                case "ZSE_Submission_2006_20060411":
                case "ZSE_Submission_2005_20041220":
                case "ZSE_Submission_2004_20040401":
                case "ZSE_Submission_2003_20030328":
                    databaseName = databaseId.Substring(0, 19); break;
                default:
                    Console.WriteLine("OOPS: Connection string for unknown database \"" + databaseId + "\" requested");
                    return null;
            }
            
            return String.Format(Private.MesapServerConnectionTemplate, Private.MesapServerName, databaseName, Private.MesapServerUsername, Private.MesapServerPassword);
        }
    }
}
