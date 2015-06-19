using System;
using System.Collections.Generic;
using System.Text;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Data;
using System.Collections;
using System.Configuration;
using USC.GISResearchLab.Common.Utils.Directories;
using System.IO;
using USC.GISResearchLab.Common.Utils.Strings;
using USC.GISResearchLab.Common.Utils.Files;
using System.Diagnostics;
using USC.GISResearchLab.Common.Diagnostics.TraceEvents;
using System.Data.OleDb;
using USC.GISResearchLab.Common.ShapeLibs;
using System.Runtime.InteropServices;

namespace USC.GISResearchLab.ShortestPath.RoadNetworkData
{
  public class Worker
    {

        #region Properties

    public TraceSource TraceSource    {      get;      set;    }

        public ArrayList Directories    {      get;      set;    }
        
        public ProgressState ProgressState    {      get;      set;    }        

        public BackgroundWorker BackgroundWorker    {      get;      set;    }

        public DoWorkEventArgs DoWorkEventArgs { get; set; }

        #endregion

        public Worker(TraceSource traceSource, BackgroundWorker backgroundWorker)
        {
            TraceSource = traceSource;
            BackgroundWorker = backgroundWorker;
        }

        public bool Run(DoWorkEventArgs e, string topDirectory, StreetSource streetSource)
        {
            bool ret = false;
            ProgressState = new ProgressState();
            DoWorkEventArgs = e;

            try
            {
                RunReader(topDirectory, streetSource);
                ret = true;
            }
            catch (Exception exc)
            {
                //// MessageBox.Show(exc.Message, "Error");
                return false;

            }
            return ret;
        }

        public void RunReader(string filename, StreetSource streetSource)
        {
            switch (streetSource)
            {
                case StreetSource.Navteq:
                    RunNavteqReader(filename);
                    break;
                case StreetSource.TeleAtlas:
                    RunTeleAtlasReader(filename);
                    break;
                default:
                    throw new Exception("Unexpected StreetSource: " + streetSource);

            }

          
        }

        public void RunNavteqReader(string filename)
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = ConfigurationSettings.AppSettings["DataSource"];
            builder.InitialCatalog = ConfigurationSettings.AppSettings["InitialCatalog"];
            builder.Password = ConfigurationSettings.AppSettings["Password"];
            builder.UserID = ConfigurationSettings.AppSettings["UserID"];
            builder.IntegratedSecurity = false;

            string connectionstr = builder.ConnectionString;
            SqlConnection connection = new SqlConnection(connectionstr);

            string[] StreetsFile;
            string[] RDMSFile;
            string[] CDMSFile;

            string[] directories = Directory.GetDirectories(filename);
            foreach (string folder in directories)
            {
                string directory = (Directory.GetDirectories(folder))[0];
                StreetsFile = Directory.GetFileSystemEntries(directory, "Streets.shp");
                RDMSFile = Directory.GetFileSystemEntries(directory, "Rdms.dbf");
                CDMSFile = Directory.GetFileSystemEntries(directory, "Cdms.dbf");

                
                #region Streets File
                
                try
                {
                    int nEntities = 0;
                    int length = 0;
                    int decimals = 0;
                    int fieldWidth = 0;

                    double[] adfMin = new double[2];
                    double[] adfMax = new double[2];
                    double[] Xarr = null, Yarr = null;

                    ShapeLib.SHPObject obj = null;
                    ShapeLib.ShapeType nShapeType = 0;
                    ShapeLib.DBFFieldType fType;

                    IntPtr ptrSHP = ShapeLib.SHPOpen(StreetsFile[0], "rb");
                    ShapeLib.SHPGetInfo(ptrSHP, ref nEntities, ref nShapeType, adfMin, adfMax);

                    IntPtr ptrDBF = ShapeLib.DBFOpen(StreetsFile[0], "rb");
                    int NoOfDBFRec = ShapeLib.DBFGetRecordCount(ptrDBF);

                    System.Text.StringBuilder strFieldName = new StringBuilder(System.String.Empty);

                    string file = Path.GetFileName(StreetsFile[0]).ToUpper();
                    string tablename = file.Substring(0, file.IndexOf('.'));

                    string cmdInsertQuery = "INSERT INTO " + tablename + " ( ";
                    string colString = "";
                    int fieldCount = ShapeLib.DBFGetFieldCount(ptrDBF);

                    for (int i = 0; i < fieldCount; i++)
                    {
                        strFieldName.Append("");
                        cmdInsertQuery += " [";
                        colString += "[";
                        fType = ShapeLib.DBFGetFieldInfo(ptrDBF, i, strFieldName, ref fieldWidth, ref decimals);
                        colString += strFieldName + "] ";
                        cmdInsertQuery += strFieldName + "], ";
                        if (fType == ShapeLib.DBFFieldType.FTDouble)
                            colString += "NUMERIC(15,6), ";
                        if (fType == ShapeLib.DBFFieldType.FTInteger)
                            colString += "INT, ";
                        if (fType == ShapeLib.DBFFieldType.FTLogical)
                            colString += "BOOL, ";
                        if (fType == ShapeLib.DBFFieldType.FTString)
                            colString += "VARCHAR(255), ";
                    }

                    colString += "[FROMLONG] NUMERIC(15,6), [FROMLAT] NUMERIC(15,6), [TOLONG] NUMERIC(15,6), [TOLAT] NUMERIC(15,6) , [LINEDATA] VARCHAR(MAX)";
                    cmdInsertQuery += "[FROMLONG] , [FROMLAT] , [TOLONG] , [TOLAT] , [LINEDATA])";
                    cmdInsertQuery += " VALUES (";

                    string createTableQuery = "CREATE TABLE " + tablename + " ( " + colString + " ) ;";

                    try
                    {
                        connection.Open();
                        SqlCommand cmd = connection.CreateCommand();
                        cmd.CommandText = "DROP TABLE " + tablename;
                        cmd.CommandType = CommandType.Text;
                        int result = cmd.ExecuteNonQuery();

                        if (connection != null && connection.State == ConnectionState.Open)
                        {
                            connection.Close();
                        }
                        //sqlAccess.SqlExecuteNonQuery("DROP TABLE " + tablename);
                    }
                    catch
                    {
                    }
                    try
                    {
                        if (connection != null && connection.State == ConnectionState.Closed)
                        {
                            connection.Open();
                        }
                        SqlCommand cmd = connection.CreateCommand();
                        cmd.CommandText = createTableQuery;
                        cmd.CommandType = CommandType.Text;
                        int result = cmd.ExecuteNonQuery();
                        if (connection != null && connection.State == ConnectionState.Open)
                        {
                            connection.Close();
                        }
                    }
                    catch (Exception exc)
                    {
                        ShapeLib.DBFClose(ptrDBF);
                        ShapeLib.SHPClose(ptrSHP);
                        // // MessageBox.Show("Cannot create table " + exc.Message);
                        return;
                    }

                    if (!BackgroundWorker.CancellationPending)
                    {
                        ProgressState.Total = nEntities;
                        TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, "running");

                        for (int count = 0; count < nEntities; count++)
                        {
                            if (!BackgroundWorker.CancellationPending)
                            {
                                TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, count.ToString());
                                ProgressState.Completed = count + 1;
                                BackgroundWorker.ReportProgress(Convert.ToInt32(ProgressState.PercentCompleted), ProgressState);

                                string cmdInsert = "";
                                obj = ShapeLib.SHPReadObject(ptrSHP, count);
                                length = obj.nVertices;

                                for (int field = 0; field < fieldCount; field++)
                                {
                                    ShapeLib.DBFFieldType type = ShapeLib.DBFGetFieldInfo(ptrDBF, field, strFieldName, ref fieldWidth, ref decimals);
                                    switch (type)
                                    {
                                        case ShapeLib.DBFFieldType.FTDouble:
                                            cmdInsert += ShapeLib.DBFReadDoubleAttribute(ptrDBF, count, field).ToString() + ",";
                                            break;
                                        case ShapeLib.DBFFieldType.FTInteger:
                                            cmdInsert += ShapeLib.DBFReadIntegerAttribute(ptrDBF, count, field).ToString() + ",";
                                            break;
                                        case ShapeLib.DBFFieldType.FTLogical:
                                            cmdInsert += ShapeLib.DBFReadLogicalAttribute(ptrDBF, count, field).ToString() + ",";
                                            break;
                                        case ShapeLib.DBFFieldType.FTString:
                                            cmdInsert += "'" + ShapeLib.DBFReadStringAttribute(ptrDBF, count, field).Replace("'", "''") + "',";
                                            break;
                                        case ShapeLib.DBFFieldType.FTInvalid:
                                        default:
                                            break;
                                    }
                                }

                                Xarr = new double[length];
                                Yarr = new double[length];

                                Marshal.Copy(obj.padfX, Xarr, 0, length);
                                Marshal.Copy(obj.padfY, Yarr, 0, length);

                                string Linedata = "";
                                for (int i = 0; i < length; i++)
                                {
                                    if (i == 0 || i == (length - 1))
                                    {
                                        cmdInsert += Xarr[i].ToString() + ", ";
                                        cmdInsert += Yarr[i].ToString() + ", ";
                                    }
                                    Linedata += Xarr[i].ToString();
                                    Linedata += " ";
                                    Linedata += Yarr[i].ToString();
                                    Linedata += " , ";
                                }
                                if (Linedata.Length >= 3)
                                    Linedata = Linedata.Remove(Linedata.Length - 3, 2);


                                cmdInsert += "'" + Linedata + "' )";

                                if (connection != null && connection.State == ConnectionState.Closed)
                                {
                                    connection.Open();
                                }
                                SqlCommand cmd = connection.CreateCommand();
                                cmd.CommandText = cmdInsertQuery + " " + cmdInsert;
                                cmd.CommandType = CommandType.Text;
                                int result = cmd.ExecuteNonQuery();
                                if (connection != null && connection.State == ConnectionState.Open)
                                {
                                    connection.Close();
                                }
                            }
                            else
                            {
                                BackgroundWorker.ReportProgress(Convert.ToInt32(ProgressState.PercentCompleted), ProgressState);
                                DoWorkEventArgs.Cancel = true;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    //ShapeLib.DBFClose(ptrDBF);
                    //ShapeLib.SHPClose(ptrSHP);
                    // MessageBox.Show(ex.Message, "Database Error");
                    return;
                }
                
                #endregion

                #region CDMS File

                try
                {
                    int nEntities = 0;
                    int decimals = 0;
                    int fieldWidth = 0;

                    ShapeLib.DBFFieldType fType;

                    IntPtr ptrDBF;
                    ptrDBF = ShapeLib.DBFOpen(CDMSFile[0], "rb");
                    int NoOfDBFRec = 0;
                    NoOfDBFRec = ShapeLib.DBFGetRecordCount(ptrDBF);

                    System.Text.StringBuilder strFieldName = new StringBuilder(System.String.Empty);

                    string file = Path.GetFileName(CDMSFile[0]).ToUpper();
                    string tablename = file.Substring(0, file.IndexOf('.'));

                    string cmdInsertQuery = "INSERT INTO " + tablename + " ( ";
                    string colString = "";
                    int fieldCount = ShapeLib.DBFGetFieldCount(ptrDBF);

                    for (int i = 0; i < fieldCount; i++)
                    {
                        strFieldName.Append("");
                        cmdInsertQuery += "[";
                        colString += "[";
                        fType = ShapeLib.DBFGetFieldInfo(ptrDBF, i, strFieldName, ref fieldWidth, ref decimals);
                        colString += strFieldName + "] ";
                        cmdInsertQuery += strFieldName + "], ";
                        if (fType == ShapeLib.DBFFieldType.FTDouble)
                            colString += "NUMERIC(15,6), ";
                        if (fType == ShapeLib.DBFFieldType.FTInteger)
                            colString += "INT, ";
                        if (fType == ShapeLib.DBFFieldType.FTLogical)
                            colString += "BOOL, ";
                        if (fType == ShapeLib.DBFFieldType.FTString)
                            colString += "VARCHAR(255), ";
                    }

                    colString = colString.Remove(colString.Length - 2, 2);
                    colString += "";

                    cmdInsertQuery = cmdInsertQuery.Remove(cmdInsertQuery.Length - 2, 2);
                    cmdInsertQuery += ")";
                    cmdInsertQuery += " VALUES (";

                    string createTableQuery = "CREATE TABLE " + tablename + " ( " + colString + " ) ;";

                    try
                    {
                        connection.Open();
                        SqlCommand cmd = connection.CreateCommand();
                        cmd.CommandText = "DROP TABLE " + tablename;
                        cmd.CommandType = CommandType.Text;
                        int result = cmd.ExecuteNonQuery();

                        if (connection != null && connection.State == ConnectionState.Open)
                        {
                            connection.Close();
                        }
                        //sqlAccess.SqlExecuteNonQuery("DROP TABLE " + tablename);
                    }
                    catch
                    {
                    }
                    try
                    {
                        if (connection != null && connection.State == ConnectionState.Closed)
                        {
                            connection.Open();
                        }
                        SqlCommand cmd = connection.CreateCommand();
                        cmd.CommandText = createTableQuery;
                        cmd.CommandType = CommandType.Text;
                        int result = cmd.ExecuteNonQuery();
                        if (connection != null && connection.State == ConnectionState.Open)
                        {
                            connection.Close();
                        }
                        //sqlAccess.SqlExecuteNonQuery(createTableQuery);
                    }
                    catch (Exception exc)
                    {
                        // MessageBox.Show("Cannot create table " + exc.Message);
                        return;
                    }

                    if (!BackgroundWorker.CancellationPending)
                    {
                        ProgressState.Total = NoOfDBFRec;
                        TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, "running");

                        for (int count = 0; count < NoOfDBFRec; count++)
                        {
                            if (!BackgroundWorker.CancellationPending)
                            {
                                TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, count.ToString());
                                ProgressState.Completed = count + 1;
                                BackgroundWorker.ReportProgress(Convert.ToInt32(ProgressState.PercentCompleted), ProgressState);

                                string cmdInsert = "";

                                for (int field = 0; field < fieldCount; field++)
                                {
                                    ShapeLib.DBFFieldType type = ShapeLib.DBFGetFieldInfo(ptrDBF, field, strFieldName, ref fieldWidth, ref decimals);
                                    switch (type)
                                    {
                                        case ShapeLib.DBFFieldType.FTDouble:
                                            cmdInsert += ShapeLib.DBFReadDoubleAttribute(ptrDBF, count, field).ToString() + ",";
                                            break;
                                        case ShapeLib.DBFFieldType.FTInteger:
                                            cmdInsert += ShapeLib.DBFReadIntegerAttribute(ptrDBF, count, field).ToString() + ",";
                                            break;
                                        case ShapeLib.DBFFieldType.FTLogical:
                                            cmdInsert += ShapeLib.DBFReadLogicalAttribute(ptrDBF, count, field).ToString() + ",";
                                            break;
                                        case ShapeLib.DBFFieldType.FTString:
                                            cmdInsert += "'" + ShapeLib.DBFReadStringAttribute(ptrDBF, count, field).Replace("'", "''") + "',";
                                            break;
                                        case ShapeLib.DBFFieldType.FTInvalid:
                                        default:
                                            break;
                                    }
                                }

                                cmdInsert = cmdInsert.Remove(cmdInsert.Length - 1, 1);
                                cmdInsert += ")";

                                if (connection != null && connection.State == ConnectionState.Closed)
                                {
                                    connection.Open();
                                }
                                SqlCommand cmd = connection.CreateCommand();
                                cmd.CommandText = cmdInsertQuery + " " + cmdInsert;
                                cmd.CommandType = CommandType.Text;
                                int result = cmd.ExecuteNonQuery();
                                if (connection != null && connection.State == ConnectionState.Open)
                                {
                                    connection.Close();
                                }
                            }
                            else
                            {
                                BackgroundWorker.ReportProgress(Convert.ToInt32(ProgressState.PercentCompleted), ProgressState);
                                DoWorkEventArgs.Cancel = true;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    // MessageBox.Show(ex.Message, "Database Error");
                    return;
                }

                #endregion
                  
                #region RDMS File

                try
                {
                    int nEntities = 0;
                    int decimals = 0;
                    int fieldWidth = 0;

                    ShapeLib.DBFFieldType fType;

                    IntPtr ptrDBF;
                    ptrDBF = ShapeLib.DBFOpen(RDMSFile[0], "rb");
                    int NoOfDBFRec = 0;
                    NoOfDBFRec = ShapeLib.DBFGetRecordCount(ptrDBF);

                    System.Text.StringBuilder strFieldName = new StringBuilder(System.String.Empty);

                    string file = Path.GetFileName(RDMSFile[0]).ToUpper();
                    string tablename = file.Substring(0, file.IndexOf('.'));

                    string cmdInsertQuery = "INSERT INTO " + tablename + " ( ";
                    string colString = "";
                    int fieldCount = ShapeLib.DBFGetFieldCount(ptrDBF);

                    for (int i = 0; i < fieldCount; i++)
                    {
                        strFieldName.Append("");
                        cmdInsertQuery += "[";
                        colString += "[";
                        fType = ShapeLib.DBFGetFieldInfo(ptrDBF, i, strFieldName, ref fieldWidth, ref decimals);
                        colString += strFieldName + "] ";
                        cmdInsertQuery += strFieldName + "], ";
                        if (fType == ShapeLib.DBFFieldType.FTDouble)
                            colString += "NUMERIC(15,6), ";
                        if (fType == ShapeLib.DBFFieldType.FTInteger)
                            colString += "INT, ";
                        if (fType == ShapeLib.DBFFieldType.FTLogical)
                            colString += "BOOL, ";
                        if (fType == ShapeLib.DBFFieldType.FTString)
                            colString += "VARCHAR(255), ";
                    }

                    colString = colString.Remove(colString.Length - 2, 2);
                    colString += "";

                    cmdInsertQuery = cmdInsertQuery.Remove(cmdInsertQuery.Length - 2, 2);
                    cmdInsertQuery += ")";
                    cmdInsertQuery += " VALUES (";

                    string createTableQuery = "CREATE TABLE " + tablename + " ( " + colString + " ) ;";

                    try
                    {
                        connection.Open();
                        SqlCommand cmd = connection.CreateCommand();
                        cmd.CommandText = "DROP TABLE " + tablename;
                        cmd.CommandType = CommandType.Text;
                        int result = cmd.ExecuteNonQuery();

                        if (connection != null && connection.State == ConnectionState.Open)
                        {
                            connection.Close();
                        }
                        //sqlAccess.SqlExecuteNonQuery("DROP TABLE " + tablename);
                    }
                    catch
                    {
                    }
                    try
                    {
                        if (connection != null && connection.State == ConnectionState.Closed)
                        {
                            connection.Open();
                        }
                        SqlCommand cmd = connection.CreateCommand();
                        cmd.CommandText = createTableQuery;
                        cmd.CommandType = CommandType.Text;
                        int result = cmd.ExecuteNonQuery();
                        if (connection != null && connection.State == ConnectionState.Open)
                        {
                            connection.Close();
                        }
                        //sqlAccess.SqlExecuteNonQuery(createTableQuery);
                    }
                    catch (Exception exc)
                    {
                        // MessageBox.Show("Cannot create table " + exc.Message);
                        return;
                    }

                    if (!BackgroundWorker.CancellationPending)
                    {
                        ProgressState.Total = NoOfDBFRec;
                        TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, "running");

                        for (int count = 0; count < NoOfDBFRec; count++)
                        {
                            if (!BackgroundWorker.CancellationPending)
                            {
                                TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, count.ToString());
                                ProgressState.Completed = count + 1;
                                BackgroundWorker.ReportProgress(Convert.ToInt32(ProgressState.PercentCompleted), ProgressState);

                                string cmdInsert = "";

                                for (int field = 0; field < fieldCount; field++)
                                {
                                    ShapeLib.DBFFieldType type = ShapeLib.DBFGetFieldInfo(ptrDBF, field, strFieldName, ref fieldWidth, ref decimals);
                                    switch (type)
                                    {
                                        case ShapeLib.DBFFieldType.FTDouble:
                                            cmdInsert += ShapeLib.DBFReadDoubleAttribute(ptrDBF, count, field).ToString() + ",";
                                            break;
                                        case ShapeLib.DBFFieldType.FTInteger:
                                            cmdInsert += ShapeLib.DBFReadIntegerAttribute(ptrDBF, count, field).ToString() + ",";
                                            break;
                                        case ShapeLib.DBFFieldType.FTLogical:
                                            cmdInsert += ShapeLib.DBFReadLogicalAttribute(ptrDBF, count, field).ToString() + ",";
                                            break;
                                        case ShapeLib.DBFFieldType.FTString:
                                            cmdInsert += "'" + ShapeLib.DBFReadStringAttribute(ptrDBF, count, field).Replace("'", "''") + "',";
                                            break;
                                        case ShapeLib.DBFFieldType.FTInvalid:
                                        default:
                                            break;
                                    }
                                }

                                cmdInsert = cmdInsert.Remove(cmdInsert.Length - 1, 1);
                                cmdInsert += ")";

                                if (connection != null && connection.State == ConnectionState.Closed)
                                {
                                    connection.Open();
                                }
                                SqlCommand cmd = connection.CreateCommand();
                                cmd.CommandText = cmdInsertQuery + " " + cmdInsert;
                                cmd.CommandType = CommandType.Text;
                                int result = cmd.ExecuteNonQuery();
                                if (connection != null && connection.State == ConnectionState.Open)
                                {
                                    connection.Close();
                                }
                            }
                            else
                            {
                                BackgroundWorker.ReportProgress(Convert.ToInt32(ProgressState.PercentCompleted), ProgressState);
                                DoWorkEventArgs.Cancel = true;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    // MessageBox.Show(ex.Message, "Database Error");
                    return;
                }

                #endregion

            }
        }

        public void RunTeleAtlasReader(string filename)
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = ConfigurationSettings.AppSettings["DataSource"];
            builder.InitialCatalog = ConfigurationSettings.AppSettings["InitialCatalog"];
            builder.Password = ConfigurationSettings.AppSettings["Password"];
            builder.UserID = ConfigurationSettings.AppSettings["UserID"];
            builder.IntegratedSecurity = false;

            string connectionstr = builder.ConnectionString;
            SqlConnection connection = new SqlConnection(connectionstr);

            string[] StreetsFile;
            string[] ManeuversFile;
            string[] TollFile;

            string[] directories = Directory.GetDirectories(filename);
            foreach (string directory in directories)
            {
                StreetsFile = Directory.GetFileSystemEntries(directory, "*st.shp");
                ManeuversFile = Directory.GetFileSystemEntries(directory, "*mn.shp");
                TollFile = Directory.GetFileSystemEntries(directory, "*tl.dbf");

                
                #region Streets File
                
                try
                {
                    int nEntities = 0;
                    int length = 0;
                    int decimals = 0;
                    int fieldWidth = 0;

                    double[] adfMin = new double[2];
                    double[] adfMax = new double[2];
                    double[] Xarr = null, Yarr = null;

                    ShapeLib.SHPObject obj = null;
                    ShapeLib.ShapeType nShapeType = 0;
                    ShapeLib.DBFFieldType fType;

                    IntPtr ptrSHP = ShapeLib.SHPOpen(StreetsFile[0], "rb");
                    ShapeLib.SHPGetInfo(ptrSHP, ref nEntities, ref nShapeType, adfMin, adfMax);

                    IntPtr ptrDBF = ShapeLib.DBFOpen(StreetsFile[0], "rb");
                    int NoOfDBFRec = ShapeLib.DBFGetRecordCount(ptrDBF);

                    System.Text.StringBuilder strFieldName = new StringBuilder(System.String.Empty);

                    string file = Path.GetFileName(StreetsFile[0]).ToUpper();
                    string tablename = file.Substring(0, file.IndexOf('.'));

                    string cmdInsertQuery = "INSERT INTO " + tablename + " ( ";
                    string colString = "";
                    int fieldCount = ShapeLib.DBFGetFieldCount(ptrDBF);

                    for (int i = 0; i < fieldCount; i++)
                    {
                        strFieldName.Append("");
                        cmdInsertQuery += " [";
                        colString += "[";
                        fType = ShapeLib.DBFGetFieldInfo(ptrDBF, i, strFieldName, ref fieldWidth, ref decimals);
                        colString += strFieldName + "] ";
                        cmdInsertQuery += strFieldName + "], ";
                        if (fType == ShapeLib.DBFFieldType.FTDouble)
                            colString += "NUMERIC(15,6), ";
                        if (fType == ShapeLib.DBFFieldType.FTInteger)
                            colString += "INT, ";
                        if (fType == ShapeLib.DBFFieldType.FTLogical)
                            colString += "BOOL, ";
                        if (fType == ShapeLib.DBFFieldType.FTString)
                            colString += "VARCHAR(255), ";
                    }

                    colString += "[FROMLONG] NUMERIC(15,6), [FROMLAT] NUMERIC(15,6), [TOLONG] NUMERIC(15,6), [TOLAT] NUMERIC(15,6) , [LINEDATA] VARCHAR(MAX)";
                    cmdInsertQuery += "[FROMLONG] , [FROMLAT] , [TOLONG] , [TOLAT] , [LINEDATA])";
                    cmdInsertQuery += " VALUES (";

                    string createTableQuery = "CREATE TABLE " + tablename + " ( " + colString + " ) ;";

                    try
                    {
                        connection.Open();
                        SqlCommand cmd = connection.CreateCommand();
                        cmd.CommandText = "DROP TABLE " + tablename;
                        cmd.CommandType = CommandType.Text;
                        int result = cmd.ExecuteNonQuery();

                        if (connection != null && connection.State == ConnectionState.Open)
                        {
                            connection.Close();
                        }
                        //sqlAccess.SqlExecuteNonQuery("DROP TABLE " + tablename);
                    }
                    catch
                    {
                    }
                    try
                    {
                        if (connection != null && connection.State == ConnectionState.Closed)
                        {
                            connection.Open();
                        }
                        SqlCommand cmd = connection.CreateCommand();
                        cmd.CommandText = createTableQuery;
                        cmd.CommandType = CommandType.Text;
                        int result = cmd.ExecuteNonQuery();
                        if (connection != null && connection.State == ConnectionState.Open)
                        {
                            connection.Close();
                        }
                    }
                    catch (Exception exc)
                    {
                        ShapeLib.DBFClose(ptrDBF);
                        ShapeLib.SHPClose(ptrSHP);
                        // MessageBox.Show("Cannot create table " + exc.Message);
                        return;
                    }

                    if (!BackgroundWorker.CancellationPending)
                    {
                        ProgressState.Total = nEntities;
                        TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, "running");

                        for (int count = 0; count < nEntities; count++)
                        {
                            if (!BackgroundWorker.CancellationPending)
                            {
                                TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, count.ToString());
                                ProgressState.Completed = count + 1;
                                BackgroundWorker.ReportProgress(Convert.ToInt32(ProgressState.PercentCompleted), ProgressState);

                                string cmdInsert = "";
                                obj = ShapeLib.SHPReadObject(ptrSHP, count);
                                length = obj.nVertices;

                                for (int field = 0; field < fieldCount; field++)
                                {
                                    ShapeLib.DBFFieldType type = ShapeLib.DBFGetFieldInfo(ptrDBF, field, strFieldName, ref fieldWidth, ref decimals);
                                    switch (type)
                                    {
                                        case ShapeLib.DBFFieldType.FTDouble:
                                            cmdInsert += ShapeLib.DBFReadDoubleAttribute(ptrDBF, count, field).ToString() + ",";
                                            break;
                                        case ShapeLib.DBFFieldType.FTInteger:
                                            cmdInsert += ShapeLib.DBFReadIntegerAttribute(ptrDBF, count, field).ToString() + ",";
                                            break;
                                        case ShapeLib.DBFFieldType.FTLogical:
                                            cmdInsert += ShapeLib.DBFReadLogicalAttribute(ptrDBF, count, field).ToString() + ",";
                                            break;
                                        case ShapeLib.DBFFieldType.FTString:
                                            cmdInsert += "'" + ShapeLib.DBFReadStringAttribute(ptrDBF, count, field).Replace("'", "''") + "',";
                                            break;
                                        case ShapeLib.DBFFieldType.FTInvalid:
                                        default:
                                            break;
                                    }
                                }

                                Xarr = new double[length];
                                Yarr = new double[length];

                                Marshal.Copy(obj.padfX, Xarr, 0, length);
                                Marshal.Copy(obj.padfY, Yarr, 0, length);

                                string Linedata = "";
                                for (int i = 0; i < length; i++)
                                {
                                    if (i == 0 || i == (length - 1))
                                    {
                                        cmdInsert += Xarr[i].ToString() + ", ";
                                        cmdInsert += Yarr[i].ToString() + ", ";
                                    }
                                    Linedata += Xarr[i].ToString();
                                    Linedata += " ";
                                    Linedata += Yarr[i].ToString();
                                    Linedata += " , ";
                                }
                                if (Linedata.Length >= 3)
                                    Linedata = Linedata.Remove(Linedata.Length - 3, 2);


                                cmdInsert += "'" + Linedata + "' )";

                                if (connection != null && connection.State == ConnectionState.Closed)
                                {
                                    connection.Open();
                                }
                                SqlCommand cmd = connection.CreateCommand();
                                cmd.CommandText = cmdInsertQuery + " " + cmdInsert;
                                cmd.CommandType = CommandType.Text;
                                int result = cmd.ExecuteNonQuery();
                                if (connection != null && connection.State == ConnectionState.Open)
                                {
                                    connection.Close();
                                }
                            }
                            else
                            {
                                BackgroundWorker.ReportProgress(Convert.ToInt32(ProgressState.PercentCompleted), ProgressState);
                                DoWorkEventArgs.Cancel = true;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    //ShapeLib.DBFClose(ptrDBF);
                    //ShapeLib.SHPClose(ptrSHP);
                    // MessageBox.Show(ex.Message, "Database Error");
                    return;
                }
                
                #endregion
              
                #region Toll File
                try
                {
                    IntPtr ptrDBF;
                    ptrDBF = ShapeLib.DBFOpen(TollFile[0], "r");
                    int NoOfDBFRec = ShapeLib.DBFGetRecordCount(ptrDBF);

                    string file = Path.GetFileName(StreetsFile[0]).ToUpper();
                    string tablename = file.Substring(0, file.IndexOf('.'));

                    if (!BackgroundWorker.CancellationPending)
                    {
                        ProgressState.Total = NoOfDBFRec;
                        TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, "running");

                        try
                        {
                            if (connection != null && connection.State == ConnectionState.Closed)
                            {
                                connection.Open();
                            }
                            SqlCommand cmd = connection.CreateCommand();
                            cmd.CommandText = "ALTER TABLE dbo." + tablename + " ADD TOLL varchar(1)";
                            cmd.CommandType = CommandType.Text;
                            int result = cmd.ExecuteNonQuery();
                            if (connection != null && connection.State == ConnectionState.Open)
                            {
                                connection.Close();
                            }
                        }
                        catch
                        {
                        }

                        for (int count = 0; count < NoOfDBFRec; count++)
                        {
                            if (!BackgroundWorker.CancellationPending)
                            {
                                TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, count.ToString());
                                ProgressState.Completed = count + 1;
                                BackgroundWorker.ReportProgress(Convert.ToInt32(ProgressState.PercentCompleted), ProgressState);

                                string cmdUpdate = "UPDATE " + tablename + " SET TOLL = ";
                                cmdUpdate += "'" + ShapeLib.DBFReadStringAttribute(ptrDBF, count, 1) +"' ";
                                cmdUpdate += " WHERE DYNAMAP_ID = " + ShapeLib.DBFReadDoubleAttribute(ptrDBF, count, 0).ToString();

                                if (connection != null && connection.State == ConnectionState.Closed)
                                {
                                    connection.Open();
                                }
                                SqlCommand cmd = connection.CreateCommand();
                                cmd.CommandText = cmdUpdate;
                                cmd.CommandType = CommandType.Text;
                                int result = cmd.ExecuteNonQuery();
                                if (connection != null && connection.State == ConnectionState.Open)
                                {
                                    connection.Close();
                                }
                            }
                            else
                            {
                                BackgroundWorker.ReportProgress(Convert.ToInt32(ProgressState.PercentCompleted), ProgressState);
                                DoWorkEventArgs.Cancel = true;
                            }
                        }
                    }
                }
                catch (Exception exc)
                {
                    // MessageBox.Show("Update Failed" + exc.Message);
                    return;
                }
                
                #endregion
                  
                #region Maneuvers File

                try
                {
                    int nEntities = 0;
                    int decimals = 0;
                    int fieldWidth = 0;

                    ShapeLib.DBFFieldType fType;

                    IntPtr ptrDBF;
                    ptrDBF = ShapeLib.DBFOpen(ManeuversFile[0], "rb");
                    int NoOfDBFRec = 0;
                    NoOfDBFRec = ShapeLib.DBFGetRecordCount(ptrDBF);

                    System.Text.StringBuilder strFieldName = new StringBuilder(System.String.Empty);

                    string file = Path.GetFileName(ManeuversFile[0]).ToUpper();
                    string tablename = file.Substring(0, file.IndexOf('.'));

                    string cmdInsertQuery = "INSERT INTO " + tablename + " ( ";
                    string colString = "";
                    int fieldCount = ShapeLib.DBFGetFieldCount(ptrDBF);

                    for (int i = 0; i < fieldCount; i++)
                    {
                      
                        strFieldName.Append("");
                        cmdInsertQuery += "[";
                        colString += "[";
                        fType = ShapeLib.DBFGetFieldInfo(ptrDBF, i, strFieldName, ref fieldWidth, ref decimals);
                        colString += strFieldName + "] ";
                        cmdInsertQuery += strFieldName + "], ";
                        if (fType == ShapeLib.DBFFieldType.FTDouble)
                            colString += "NUMERIC(15,6), ";
                        if (fType == ShapeLib.DBFFieldType.FTInteger)
                            colString += "INT, ";
                        if (fType == ShapeLib.DBFFieldType.FTLogical)
                            colString += "BOOL, ";
                        if (fType == ShapeLib.DBFFieldType.FTString)
                            colString += "VARCHAR(255), ";
                    }

                    colString = colString.Remove(colString.Length - 2, 2);
                    colString += "";

                    cmdInsertQuery = cmdInsertQuery.Remove(cmdInsertQuery.Length - 2, 2);
                    cmdInsertQuery += ")";
                    cmdInsertQuery += " VALUES (";

                    string createTableQuery = "CREATE TABLE " + tablename + " ( " + colString + " ) ;";

                    try
                    {
                        connection.Open();
                        SqlCommand cmd = connection.CreateCommand();
                        cmd.CommandText = "DROP TABLE " + tablename;
                        cmd.CommandType = CommandType.Text;
                        int result = cmd.ExecuteNonQuery();

                        if (connection != null && connection.State == ConnectionState.Open)
                        {
                            connection.Close();
                        }
                    }
                    catch
                    {
                    }
                    try
                    {
                        if (connection != null && connection.State == ConnectionState.Closed)
                        {
                            connection.Open();
                        }
                        SqlCommand cmd = connection.CreateCommand();
                        cmd.CommandText = createTableQuery;
                        cmd.CommandType = CommandType.Text;
                        int result = cmd.ExecuteNonQuery();
                        if (connection != null && connection.State == ConnectionState.Open)
                        {
                            connection.Close();
                        }
                    }
                    catch (Exception exc)
                    {
                        // MessageBox.Show("Cannot create table " + exc.Message);
                        return;
                    }

                    if (!BackgroundWorker.CancellationPending)
                    {
                        ProgressState.Total = NoOfDBFRec;
                        TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, "running");

                        for (int count = 0; count < NoOfDBFRec; count++)
                        {
                            if (!BackgroundWorker.CancellationPending)
                            {
                                TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, count.ToString());
                                ProgressState.Completed = count + 1;
                                BackgroundWorker.ReportProgress(Convert.ToInt32(ProgressState.PercentCompleted), ProgressState);

                                string cmdInsert = "";

                                for (int field = 0; field < fieldCount; field++)
                                {
                                    ShapeLib.DBFFieldType type = ShapeLib.DBFGetFieldInfo(ptrDBF, field, strFieldName, ref fieldWidth, ref decimals);
                                    switch (type)
                                    {
                                        case ShapeLib.DBFFieldType.FTDouble:
                                            cmdInsert += ShapeLib.DBFReadDoubleAttribute(ptrDBF, count, field).ToString() + ",";
                                            break;
                                        case ShapeLib.DBFFieldType.FTInteger:
                                            cmdInsert += ShapeLib.DBFReadIntegerAttribute(ptrDBF, count, field).ToString() + ",";
                                            break;
                                        case ShapeLib.DBFFieldType.FTLogical:
                                            cmdInsert += ShapeLib.DBFReadLogicalAttribute(ptrDBF, count, field).ToString() + ",";
                                            break;
                                        case ShapeLib.DBFFieldType.FTString:
                                            cmdInsert += "'" + ShapeLib.DBFReadStringAttribute(ptrDBF, count, field).Replace("'", "''") + "',";
                                            break;
                                        case ShapeLib.DBFFieldType.FTInvalid:
                                        default:
                                            break;
                                    }
                                }

                                cmdInsert = cmdInsert.Remove(cmdInsert.Length - 1, 1);
                                cmdInsert += ")";

                                if (connection != null && connection.State == ConnectionState.Closed)
                                {
                                    connection.Open();
                                }
                                SqlCommand cmd = connection.CreateCommand();
                                cmd.CommandText = cmdInsertQuery + " " + cmdInsert;
                                cmd.CommandType = CommandType.Text;
                                int result = cmd.ExecuteNonQuery();
                                if (connection != null && connection.State == ConnectionState.Open)
                                {
                                    connection.Close();
                                }
                            }
                            else
                            {
                                BackgroundWorker.ReportProgress(Convert.ToInt32(ProgressState.PercentCompleted), ProgressState);
                                DoWorkEventArgs.Cancel = true;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    // MessageBox.Show(ex.Message, "Database Error");
                    return;
                }

                #endregion

            }

        }
    }
}