using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Xml.Serialization;
using USC.GISResearchLab.Common.Core.Configurations.Interfaces;

namespace USC.GISResearchLab.ShortestPath.RoadNetworkData
{
    public delegate void DoEventsDelegation();

    public enum SQLVersionEnum
    {
        SQLServer2008,
        SQLServer2005,
        Unknown
    }

    [Serializable]
    public class RoadNetworkDBManagerInput : IConfiguration
    {
        public static readonly string MasterTableName = "AvailableRoadNetworkData";
        public ShapeFileImporterInput ImporterInput;
        public string LogFileBase;
        public bool LogEnabled;
        public string SQLDataSource { get; set; }
        public string UserTableDBName { get; set; }
        public string WebAppDBName { get; set; }
        public string SQLInitialCatalog { get; set; }
        public string SQLUserID { get; set; }
        public SQLVersionEnum SQLVersion { get; set; }
        [XmlIgnore]
        public string SQLPassword { get; set; }
        [XmlIgnore]
        public string DatabaseToModify;
        [XmlIgnore]
        public List<string> DatabasesToMerge;
        [XmlIgnore]
        public DoEventsDelegation DoEventsMethod;
        public string MyName { get; set; }

        [XmlIgnore]
        public TraceSource MyTraceSource { get; set; }

        public string SQLConnectionString
        {
            get
            {
                string conStr = "";
                try
                {
                    conStr = GetConnectionString(SQLInitialCatalog);
                }
                catch
                {
                }
                return conStr;
            }
        }

        public string GetConnectionString(string initCatalog)
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = SQLDataSource;
            builder.InitialCatalog = initCatalog;
            builder.Password = SQLPassword;
            builder.UserID = SQLUserID;
            builder.IntegratedSecurity = false;
            builder.AsynchronousProcessing = true;
            return builder.ConnectionString;
        }

        /// <summary>
        /// A summery of what this object is holding as an string. Good for log file.
        /// </summary>
        public override string ToString()
        {
            string o = "Database to modify = " + this.DatabaseToModify;
            o += "; Log file enabled = " + this.LogEnabled;
            o += "; Log Files = " + this.LogFileBase + ".log, " + this.LogFileBase + "_bug.log";
            o += "; Master Table Name = " + RoadNetworkDBManagerInput.MasterTableName;
            o += "; SQL Data Source = " + this.SQLDataSource;
            o += "; SQL Initial Catalog = " + this.SQLInitialCatalog;
            o += "; SQL User = " + this.SQLUserID;
            o += "; Import Data Description = " + this.ImporterInput.DataDescription;
            o += "; Import Data Year = " + this.ImporterInput.DataYear;
            o += "; Import Data Provider = " + this.ImporterInput.MyDataProvider;
            o += "; Import Database name = " + this.ImporterInput.RoadNetworkDatabaseName;
            o += "; Import Directory = " + this.ImporterInput.RootDirectory;
            o += "; Import Set As Primary = " + this.ImporterInput.SetAsPrimary;
            o += "; WebApp Database Name = " + this.WebAppDBName;
            o += "; User Tables Database Name = " + this.UserTableDBName;
            o += "; SQL Version = " + this.SQLVersion;
            return o;
        }

        /// <summary>
        /// This should be called only when you don't want to deserialize the configuration but instead you want a fresh copy
        /// </summary>
        public void SetToDefault()
        {
            SQLDataSource = "sqlserver";
            SQLInitialCatalog = "shortpath";
            SQLUserID = "sa";
            LogFileBase = string.Empty;
            LogEnabled = true;
            this.ImporterInput = new ShapeFileImporterInput();
            this.ImporterInput.MyDataProvider = DataProvider.Navteq;
            this.ImporterInput.RootDirectory = @"c:\";
            DoEventsMethod = null;
            SQLVersion = SQLVersionEnum.Unknown;
            WebAppDBName = "SPWebApp";
            UserTableDBName = "SPUserTables";
            MyName = string.Empty;
        }

        public static string CheckSQLConnection(string newConStr)
        {
            string msg = "";
            SqlConnection con = null;
            try
            {
                con = new SqlConnection(newConStr);
                con.Open();
            }
            catch (Exception ex)
            {
                msg = ex.Message;
            }
            finally
            {
                if (con != null) con.Close();
            }
            return msg;
        }
    }
}