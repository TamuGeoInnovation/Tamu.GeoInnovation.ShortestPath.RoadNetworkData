using System;
using System.Diagnostics;
using USC.GISResearchLab.Common.Diagnostics.TraceEvents;
using USC.GISResearchLab.Common.ShapeLibs;
using System.IO;
using System.ComponentModel;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Text;
using System.Runtime.InteropServices;

namespace USC.GISResearchLab.ShortestPath.RoadNetworkData
{
  public class TeleAtlasImporter : ShapeFileImporter
  {
    public TeleAtlasImporter(BackgroundWorker backgroundWorker, RoadNetworkDBManagerInput input)
      : base(backgroundWorker, input) { }

    public override ShapeFileImporterOutput SaveToSQL()
    {
      ShapeFileImporterOutput ret = new ShapeFileImporterOutput();
      MyProgressState = new ProgressState();
      string msg = "";
      IntPtr ptrSHP = IntPtr.Zero;
      IntPtr ptrDBF = IntPtr.Zero;
      try
      {
        if (myInput.MyTraceSource != null)
        {
          myInput.MyTraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Started, DateTime.Now.ToLongTimeString() + ": TeleAtlasImporter SaveToSQL Start");
          myInput.MyTraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Running, "TeleAtlasImporter SaveToSQL Configuration: " + myInput.ToString());
        }
        SqlConnection connection = new SqlConnection(myInput.SQLConnectionString);

        string[] StreetsFile;
        string[] ManeuversFile;
        string[] TollFile;

        string[] directories = Directory.GetDirectories(myInput.ImporterInput.RootDirectory);
        foreach (string directory in directories)
        {
          StreetsFile = Directory.GetFileSystemEntries(directory, "*st.shp");
          ManeuversFile = Directory.GetFileSystemEntries(directory, "*mn.shp");
          TollFile = Directory.GetFileSystemEntries(directory, "*tl.dbf");
          if (StreetsFile.Length == 0) continue;

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

            ptrSHP = ShapeLib.SHPOpen(StreetsFile[0], "rb");
            ShapeLib.SHPGetInfo(ptrSHP, ref nEntities, ref nShapeType, adfMin, adfMax);

            ptrDBF = ShapeLib.DBFOpen(StreetsFile[0], "rb");
            int NoOfDBFRec = ShapeLib.DBFGetRecordCount(ptrDBF);

            StringBuilder strFieldName = new StringBuilder(System.String.Empty);

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
            catch (Exception ex)
            {
              if (ptrDBF != IntPtr.Zero) ShapeLib.DBFClose(ptrDBF);
              if (ptrSHP != IntPtr.Zero) ShapeLib.SHPClose(ptrSHP);
              msg = "TeleAtlasImporter SaveToSQL: An error occured during process: " + ex.ToString();
              if (myInput.MyTraceSource != null)
                myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
            }

            if (!MyWorker.CancellationPending)
            {
              MyProgressState.Total = nEntities;
              myInput.MyTraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Running, "running");

              for (int count = 0; count < nEntities; count++)
              {
                if (!MyWorker.CancellationPending)
                {
                  myInput.MyTraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, count.ToString());
                  MyProgressState.Completed = count + 1;
                  MyWorker.ReportProgress(Convert.ToInt32(MyProgressState.PercentCompleted), MyProgressState);

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
                  MyWorker.ReportProgress(Convert.ToInt32(MyProgressState.PercentCompleted), MyProgressState);
                  myInput.ImporterInput.MyWorkerEventArgument.Cancel = true;
                }
              }
            }
          }
          catch (Exception ex)
          {
            if (ptrDBF != IntPtr.Zero) ShapeLib.DBFClose(ptrDBF);
            if (ptrSHP != IntPtr.Zero) ShapeLib.SHPClose(ptrSHP);
            msg = "TeleAtlasImporter SaveToSQL: An error occured during process: " + ex.ToString();
            if (myInput.MyTraceSource != null)
              myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
          }

          #endregion

          #region Toll File
          try
          {
            ptrDBF = ShapeLib.DBFOpen(TollFile[0], "r");
            int NoOfDBFRec = ShapeLib.DBFGetRecordCount(ptrDBF);

            string file = Path.GetFileName(StreetsFile[0]).ToUpper();
            string tablename = file.Substring(0, file.IndexOf('.'));

            if (!MyWorker.CancellationPending)
            {
              MyProgressState.Total = NoOfDBFRec;
              myInput.MyTraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, "running");

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
                if (!MyWorker.CancellationPending)
                {
                  myInput.MyTraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, count.ToString());
                  MyProgressState.Completed = count + 1;
                  MyWorker.ReportProgress(Convert.ToInt32(MyProgressState.PercentCompleted), MyProgressState);

                  string cmdUpdate = "UPDATE " + tablename + " SET TOLL = ";
                  cmdUpdate += "'" + ShapeLib.DBFReadStringAttribute(ptrDBF, count, 1) + "' ";
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
                  MyWorker.ReportProgress(Convert.ToInt32(MyProgressState.PercentCompleted), MyProgressState);
                  myInput.ImporterInput.MyWorkerEventArgument.Cancel = true;
                }
              }
            }
          }
          catch (Exception ex)
          {
            msg = "TeleAtlasImporter SaveToSQL: An error occured during process: " + ex.ToString();
            if (myInput.MyTraceSource != null)
              myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
          }

          #endregion

          #region Maneuvers File

          try
          {
            int decimals = 0;
            int fieldWidth = 0;

            ShapeLib.DBFFieldType fType;

            ptrDBF = ShapeLib.DBFOpen(ManeuversFile[0], "rb");
            int NoOfDBFRec = 0;
            NoOfDBFRec = ShapeLib.DBFGetRecordCount(ptrDBF);

            StringBuilder strFieldName = new StringBuilder(System.String.Empty);

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
            catch (Exception ex)
            {
              msg = "TeleAtlasImporter SaveToSQL: An error occured during process: " + ex.ToString();
              if (myInput.MyTraceSource != null)
                myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
            }

            if (!MyWorker.CancellationPending)
            {
              MyProgressState.Total = NoOfDBFRec;
              myInput.MyTraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, "running");

              for (int count = 0; count < NoOfDBFRec; count++)
              {
                if (!MyWorker.CancellationPending)
                {
                  myInput.MyTraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing, count.ToString());
                  MyProgressState.Completed = count + 1;
                  MyWorker.ReportProgress(Convert.ToInt32(MyProgressState.PercentCompleted), MyProgressState);

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
                  MyWorker.ReportProgress(Convert.ToInt32(MyProgressState.PercentCompleted), MyProgressState);
                  myInput.ImporterInput.MyWorkerEventArgument.Cancel = true;
                }
              }
            }
          }
          catch (Exception ex)
          {
            msg = "TeleAtlasImporter SaveToSQL: An error occured during process: " + ex.ToString();
            if (myInput.MyTraceSource != null)
              myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
          }

          #endregion

        }
      }
      catch (Exception ex)
      {
        msg = "TeleAtlasImporter SaveToSQL: An error occured during process: " + ex.ToString();
        if (myInput.MyTraceSource != null)
          myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
      }
      finally
      {
        if (ptrDBF != IntPtr.Zero) ShapeLib.DBFClose(ptrDBF);
        if (ptrSHP != IntPtr.Zero) ShapeLib.SHPClose(ptrSHP);
        if (myInput.MyTraceSource != null)
          myInput.MyTraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completed,
          DateTime.Now.ToLongTimeString() + ": TeleAtlasImporter SaveToSQL completed");
      }
      return ret;
    }

    public override ShapeFileImporterOutput SaveToDisk()
    {
      throw new NotImplementedException();
    }
  }
}