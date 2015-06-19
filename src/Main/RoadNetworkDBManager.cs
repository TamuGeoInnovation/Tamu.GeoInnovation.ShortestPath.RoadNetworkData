using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using USC.GISResearchLab.Common.Diagnostics.TraceEvents;

namespace USC.GISResearchLab.ShortestPath.RoadNetworkData
{
    public delegate void RoadNetworkDBManagerEventHandler(object sender, RoadNetworkDBManagerEventArg a);

    public class RoadNetworkDBManagerEventArg : EventArgs
    {
        public bool IsComplete;
        public int Progress;
        public string Status;

        public RoadNetworkDBManagerEventArg(bool complete, int progress, string status)
        {
            IsComplete = complete;
            Progress = progress;
            Status = status;
        }
    }

    public class RoadNetworkDBManager
    {
        private RoadNetworkDBManagerInput myInput;
        private BackgroundWorker myWorker;
        SqlDataAdapter adpt;
        List<string> cachedNameList;
        public event RoadNetworkDBManagerEventHandler ImportProgressChanged;
        public bool IsBusy
        {
            get
            {
                if (myWorker != null) return myWorker.IsBusy;
                else return false;
            }
        }

        public RoadNetworkDBManager(RoadNetworkDBManagerInput input)
        {
            myInput = input;
            cachedNameList = null;
            myWorker = new BackgroundWorker();
            myWorker.WorkerReportsProgress = true;
            myWorker.WorkerSupportsCancellation = true;
            myWorker.DoWork += new DoWorkEventHandler(myWorker_DoWork);
            myWorker.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_RunWorkerCompleted);
            myWorker.ProgressChanged += new ProgressChangedEventHandler(myWorker_ProgressChanged);
            if (myInput.MyTraceSource != null)
                myInput.MyTraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Started, Environment.NewLine + Environment.NewLine + DateTime.Now.ToShortDateString() + " " + DateTime.Now.ToShortTimeString() + ": RoadNetworkManager Engine Loaded");
        }

        public DataTable ListAvailableDatabases()
        {
            if (myInput.MyTraceSource != null)
                myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Enter, DateTime.Now.ToShortDateString() + " " + DateTime.Now.ToShortTimeString() + ": ListAvailableDatabases stared");
            DataTable list = null;
            string selectcomand = "select associatedTableName as Name, provider + ' ' + providerversion AS Provider, dateAdded, [description] as Comment, isPrimary, recordCount, isErr from " + RoadNetworkDBManagerInput.MasterTableName + " order by dateAdded desc";
            adpt = new SqlDataAdapter(selectcomand, myInput.SQLConnectionString);
            list = new DataTable();
            adpt.Fill(list);
            if (myInput.MyTraceSource != null)
                myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Exit, DateTime.Now.ToLongTimeString() + ": ListAvailableDatabases ended");
            return list;
        }

        public void DeleteDatabase()
        {
            if (myInput.MyTraceSource != null)
                myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Enter, DateTime.Now.ToShortDateString() + " " + DateTime.Now.ToShortTimeString() + ": DeleteDatabase stared");
            var con = new SqlConnection(myInput.SQLConnectionString);
            con.Open();
            var removeCommand = con.CreateCommand();
            var trans = con.BeginTransaction("remove");

            removeCommand.Connection = con;
            removeCommand.Transaction = trans;
            try
            {
                removeCommand.CommandText = "IF OBJECT_ID('" + myInput.DatabaseToModify + "', 'U') IS NOT NULL DROP TABLE " + myInput.DatabaseToModify;
                removeCommand.ExecuteNonQuery();
                // only in navteq mode we need to execute the 4 following commands
                removeCommand.CommandText = "IF OBJECT_ID('" + myInput.DatabaseToModify + "_Streets', 'U') IS NOT NULL DROP TABLE " + myInput.DatabaseToModify + "_Streets";
                removeCommand.ExecuteNonQuery();
                removeCommand.CommandText = "IF OBJECT_ID('" + myInput.DatabaseToModify + "_CDMS', 'U') IS NOT NULL DROP TABLE " + myInput.DatabaseToModify + "_CDMS";
                removeCommand.ExecuteNonQuery();
                removeCommand.CommandText = "IF OBJECT_ID('" + myInput.DatabaseToModify + "_RDMS', 'U') IS NOT NULL DROP TABLE " + myInput.DatabaseToModify + "_RDMS";
                removeCommand.ExecuteNonQuery();
                removeCommand.CommandText = "IF OBJECT_ID('" + myInput.DatabaseToModify + "_PreComp', 'U') IS NOT NULL DROP TABLE " + myInput.DatabaseToModify + "_PreComp";
                removeCommand.ExecuteNonQuery();
                // -------
                removeCommand.CommandText = "delete from " + RoadNetworkDBManagerInput.MasterTableName + " where associatedTableName = '" + myInput.DatabaseToModify + "'";
                removeCommand.ExecuteNonQuery();
                trans.Commit();
            }
            catch (Exception ex)
            {
                trans.Rollback("remove");
                if (myInput.MyTraceSource != null)
                {
                    string message = DateTime.Now.ToLongTimeString() + ": " + ex.ToString();
                    myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, message);
                }
            }
            finally
            {
                con.Close();
                if (myInput.MyTraceSource != null)
                    myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Exit, DateTime.Now.ToLongTimeString() + ": DeleteDatabase ended");
            }
        }

        public string MergeDataBasesValidate()
        {
            try
            {
                var con = new SqlConnection(myInput.SQLConnectionString);
                string whereCluase = string.Empty;
                con.Open();
                var validCommand = con.CreateCommand();
                validCommand.Connection = con;

                if (myInput.DatabasesToMerge.Count < 2) return "You need to select at least 2 databases to merge.";
                for (int i = 0; i < myInput.DatabasesToMerge.Count; i++)
                {
                    if (i != 0) whereCluase += " or ";
                    whereCluase += "(associatedTableName = '" + myInput.DatabasesToMerge[i] + "')";
                }
                validCommand.CommandText = "select count(*) from " + RoadNetworkDBManagerInput.MasterTableName + " where (isErr <> 0) and (" + whereCluase + ")";
                if ((int)(validCommand.ExecuteScalar()) != 0) return "One of the databases is corrupted and cannot be merged.";
                validCommand.CommandText = "select count(distinct(provider + ' ' + providerversion)) from " + RoadNetworkDBManagerInput.MasterTableName + " where (" + whereCluase + ")";
                if ((int)(validCommand.ExecuteScalar()) != 1) return "Selected databases are from different provider/version and cannot be merged.";
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
            return string.Empty;
        }

        public void MergeDatabases()
        {
            if (myInput.MyTraceSource != null)
                myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Enter, DateTime.Now.ToShortDateString() + " " + DateTime.Now.ToShortTimeString() + ": MergeDatabases stared");
            var con = new SqlConnection(myInput.SQLConnectionString + ";Asynchronous Processing=true");
            string whereCluase = string.Empty;
            bool[] isStreetExistList = null, isCDMSExistList = null, isRDMSExistList = null, isPreCompExistList = null;
            bool isPrimery = false;
            string dateAdded = string.Empty;
            int newCount = 0;
            IAsyncResult sqlResult = null;
            con.Open();
            var mergeCommand = con.CreateCommand();
            var checkCommand = con.CreateCommand();
            checkCommand.Connection = con;
            mergeCommand.Connection = con;

            // checkup
            // string msg = MergeDataBasesValidate();
            // if (msg != string.Empty) throw new Exception(msg);

            for (int i = 0; i < myInput.DatabasesToMerge.Count; i++)
            {
                if (i != 0) whereCluase += " or ";
                whereCluase += "(associatedTableName = '" + myInput.DatabasesToMerge[i] + "')";
            }
            checkCommand.CommandText = "select count(*) from " + RoadNetworkDBManagerInput.MasterTableName + " where (isPrimary = 1) and (" + whereCluase + ")";
            isPrimery = ((int)(checkCommand.ExecuteScalar()) > 0);
            checkCommand.CommandText = "select max([dateAdded]) from " + RoadNetworkDBManagerInput.MasterTableName + " where (" + whereCluase + ")";
            dateAdded = checkCommand.ExecuteScalar().ToString();

            checkCommand.CommandTimeout = 0;
            isStreetExistList = new bool[myInput.DatabasesToMerge.Count];
            isCDMSExistList = new bool[myInput.DatabasesToMerge.Count];
            isRDMSExistList = new bool[myInput.DatabasesToMerge.Count];
            isPreCompExistList = new bool[myInput.DatabasesToMerge.Count];

            for (int j = 0; j < myInput.DatabasesToMerge.Count; j++)
            {
                checkCommand.CommandText = "select OBJECT_ID('" + myInput.DatabasesToMerge[j] + "_Streets', 'U')";
                isStreetExistList[j] = (checkCommand.ExecuteScalar().ToString() != string.Empty);
                checkCommand.CommandText = "select OBJECT_ID('" + myInput.DatabasesToMerge[j] + "_CDMS', 'U')";
                isCDMSExistList[j] = (checkCommand.ExecuteScalar().ToString() != string.Empty);
                checkCommand.CommandText = "select OBJECT_ID('" + myInput.DatabasesToMerge[j] + "_RDMS', 'U')";
                isRDMSExistList[j] = (checkCommand.ExecuteScalar().ToString() != string.Empty);
                checkCommand.CommandText = "select OBJECT_ID('" + myInput.DatabasesToMerge[j] + "_PreComp', 'U')";
                isPreCompExistList[j] = (checkCommand.ExecuteScalar().ToString() != string.Empty);
            }
            var trans = con.BeginTransaction("merge");
            try
            {
                // Merge
                mergeCommand.Transaction = trans;
                mergeCommand.CommandTimeout = 0;
                for (int i = 1; i < myInput.DatabasesToMerge.Count; i++)
                {
                    if (!isStreetExistList[0])
                    {
                        if (isStreetExistList[i])
                        {
                            mergeCommand.CommandText = "EXEC sp_rename '" + myInput.DatabasesToMerge[i] + "_Streets', '" + myInput.DatabasesToMerge[0] + "_Streets'";
                            mergeCommand.ExecuteNonQuery();
                            isStreetExistList[0] = true;
                        }
                    }
                    else
                    {
                        if (isStreetExistList[i])
                        {
                            mergeCommand.CommandText = "insert into " + myInput.DatabasesToMerge[0] + "_Streets" + Environment.NewLine +
                                "select * from " + myInput.DatabasesToMerge[i] + "_Streets a where a.link_id not in " +
                                "(select b.link_id from " + myInput.DatabasesToMerge[0] + "_Streets b where b.link_id = a.link_id)";
                            sqlResult = mergeCommand.BeginExecuteNonQuery();
                            while (!sqlResult.IsCompleted)
                            {
                                if (myInput.DoEventsMethod != null) myInput.DoEventsMethod();
                                System.Threading.Thread.Sleep(500);
                            }
                            mergeCommand.EndExecuteNonQuery(sqlResult);
                            isStreetExistList[0] = true;
                        }
                    }
                    // -----------------
                    if (!isCDMSExistList[0])
                    {
                        if (isCDMSExistList[i])
                        {
                            mergeCommand.CommandText = "EXEC sp_rename '" + myInput.DatabasesToMerge[i] + "_CDMS', '" + myInput.DatabasesToMerge[0] + "_CDMS'";
                            mergeCommand.ExecuteNonQuery();
                            isCDMSExistList[0] = true;
                        }
                    }
                    else
                    {
                        if (isCDMSExistList[i])
                        {
                            mergeCommand.CommandText = "insert into " + myInput.DatabasesToMerge[0] + "_CDMS" + Environment.NewLine +
                                "select * from " + myInput.DatabasesToMerge[i] + "_CDMS a where a.link_id not in " +
                                "(select b.link_id from " + myInput.DatabasesToMerge[0] + "_CDMS b where (b.link_id = a.link_id) and (b.cond_id = a.cond_id)) " +
                                "and a.cond_id not in (select b.cond_id from " + myInput.DatabasesToMerge[0] + "_CDMS b where (b.link_id = a.link_id) and (b.cond_id = a.cond_id))";
                            sqlResult = mergeCommand.BeginExecuteNonQuery();
                            while (!sqlResult.IsCompleted)
                            {
                                if (myInput.DoEventsMethod != null) myInput.DoEventsMethod();
                                System.Threading.Thread.Sleep(500);
                            }
                            mergeCommand.EndExecuteNonQuery(sqlResult);
                            isCDMSExistList[0] = true;
                        }
                    }
                    // -----------------
                    if (!isRDMSExistList[0])
                    {
                        if (isRDMSExistList[i])
                        {
                            mergeCommand.CommandText = "EXEC sp_rename '" + myInput.DatabasesToMerge[i] + "_RDMS', '" + myInput.DatabasesToMerge[0] + "_RDMS'";
                            mergeCommand.ExecuteNonQuery();
                            isRDMSExistList[0] = true;
                        }
                    }
                    else
                    {
                        if (isRDMSExistList[i])
                        {
                            mergeCommand.CommandText = "insert into " + myInput.DatabasesToMerge[0] + "_RDMS" + Environment.NewLine +
                                "select * from " + myInput.DatabasesToMerge[i] + "_RDMS a where a.link_id not in " +
                                "(select b.link_id from " + myInput.DatabasesToMerge[0] + "_RDMS b where (b.link_id = a.link_id) and (b.cond_id = a.cond_id) and (b.seq_number = a.seq_number)) " +
                                "and a.cond_id not in (select b.cond_id from " + myInput.DatabasesToMerge[0] + "_RDMS b where (b.link_id = a.link_id) and (b.cond_id = a.cond_id) and (b.seq_number = a.seq_number))" +
                                "and a.seq_number not in (select b.seq_number from " + myInput.DatabasesToMerge[0] + "_RDMS b where (b.link_id = a.link_id) and (b.cond_id = a.cond_id) and (b.seq_number = a.seq_number))";
                            sqlResult = mergeCommand.BeginExecuteNonQuery();
                            while (!sqlResult.IsCompleted)
                            {
                                if (myInput.DoEventsMethod != null) myInput.DoEventsMethod();
                                System.Threading.Thread.Sleep(500);
                            }
                            mergeCommand.EndExecuteNonQuery(sqlResult);
                            isRDMSExistList[0] = true;
                        }
                    }
                    // -----------------
                    if (!isPreCompExistList[0])
                    {
                        if (isPreCompExistList[i])
                        {
                            mergeCommand.CommandText = "EXEC sp_rename '" + myInput.DatabasesToMerge[i] + "_PreComp', '" + myInput.DatabasesToMerge[0] + "_PreComp'";
                            mergeCommand.ExecuteNonQuery();
                            isPreCompExistList[0] = true;
                        }
                    }
                    else
                    {
                        if (isPreCompExistList[i])
                        {
                            // TODO: Merge the precomp tables here
                            /*
                            mergeCommand.CommandText = "insert into " + myInput.DatabasesToMerge[0] + "_PreComp" + Environment.NewLine +
                                "select * from " + myInput.DatabasesToMerge[i] + "_PreComp a where a.link_id not in " +
                                "(select b.link_id from " + myInput.DatabasesToMerge[0] + "_PreComp b where (b.link_id = a.link_id) and (b.cond_id = a.cond_id) and (b.seq_number = a.seq_number)) " +
                                "and a.cond_id not in (select b.cond_id from " + myInput.DatabasesToMerge[0] + "_RDMS b where (b.link_id = a.link_id) and (b.cond_id = a.cond_id) and (b.seq_number = a.seq_number))" +
                                "and a.seq_number not in (select b.seq_number from " + myInput.DatabasesToMerge[0] + "_RDMS b where (b.link_id = a.link_id) and (b.cond_id = a.cond_id) and (b.seq_number = a.seq_number))";              
                            sqlResult = mergeCommand.BeginExecuteNonQuery();
                            while (!sqlResult.IsCompleted)
                            {
                              if (myInput.DoEventsMethod != null) myInput.DoEventsMethod();
                              System.Threading.Thread.Sleep(500);
                            }
                            mergeCommand.EndExecuteNonQuery(sqlResult);
                            isPreCompExistList[0] = true;
                            */
                        }
                    }
                }
                for (int i = 1; i < myInput.DatabasesToMerge.Count; i++)
                {
                    mergeCommand.CommandText = "IF OBJECT_ID('" + myInput.DatabasesToMerge[i] + "_Streets', 'U') IS NOT NULL DROP TABLE " + myInput.DatabasesToMerge[i] + "_Streets";
                    mergeCommand.ExecuteNonQuery();
                    mergeCommand.CommandText = "IF OBJECT_ID('" + myInput.DatabasesToMerge[i] + "_CDMS', 'U') IS NOT NULL DROP TABLE " + myInput.DatabasesToMerge[i] + "_CDMS";
                    mergeCommand.ExecuteNonQuery();
                    mergeCommand.CommandText = "IF OBJECT_ID('" + myInput.DatabasesToMerge[i] + "_RDMS', 'U') IS NOT NULL DROP TABLE " + myInput.DatabasesToMerge[i] + "_RDMS";
                    mergeCommand.ExecuteNonQuery();
                    mergeCommand.CommandText = "IF OBJECT_ID('" + myInput.DatabasesToMerge[i] + "_PreComp', 'U') IS NOT NULL DROP TABLE " + myInput.DatabasesToMerge[i] + "_PreComp";
                    mergeCommand.ExecuteNonQuery();
                    mergeCommand.CommandText = "delete from " + RoadNetworkDBManagerInput.MasterTableName + " where associatedTableName = '" + myInput.DatabasesToMerge[i] + "'";
                    mergeCommand.ExecuteNonQuery();
                }
                if (isPrimery)
                {
                    mergeCommand.CommandText = "UPDATE " + RoadNetworkDBManagerInput.MasterTableName + " SET isPrimary = 1  WHERE associatedTableName = '" + myInput.DatabasesToMerge[0] + "'";
                    mergeCommand.ExecuteNonQuery();
                }
                try
                {
                    trans.Commit();
                }
                catch (Exception ex)
                {
                    trans.Rollback("merge");
                    throw new Exception("The merge transaction failed to commit.", ex);
                }
                checkCommand.CommandText = "select count(*) from " + myInput.DatabasesToMerge[0] + "_Streets";
                newCount = (int)(checkCommand.ExecuteScalar());
                checkCommand.CommandText = "select count(*) from " + myInput.DatabasesToMerge[0] + "_CDMS";
                newCount += (int)(checkCommand.ExecuteScalar());
                checkCommand.CommandText = "select count(*) from " + myInput.DatabasesToMerge[0] + "_RDMS";
                newCount += (int)(checkCommand.ExecuteScalar());
                checkCommand.CommandText = "UPDATE " + RoadNetworkDBManagerInput.MasterTableName + " SET recordCount = " + newCount + "  WHERE associatedTableName = '" + myInput.DatabasesToMerge[0] + "'";
                checkCommand.ExecuteNonQuery();
                checkCommand.CommandText = "UPDATE " + RoadNetworkDBManagerInput.MasterTableName + " SET [dateAdded] = '" + dateAdded + "'  WHERE associatedTableName = '" + myInput.DatabasesToMerge[0] + "'";
                checkCommand.ExecuteNonQuery();
                checkCommand.CommandText = "UPDATE " + RoadNetworkDBManagerInput.MasterTableName + " SET [isErr] = " + 0 + "  WHERE associatedTableName = '" + myInput.DatabasesToMerge[0] + "'";
                checkCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                if (myInput.MyTraceSource != null)
                {
                    string message = DateTime.Now.ToLongTimeString() + ": " + ex.ToString();
                    myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, message);
                }
            }
            finally
            {
                con.Close();
                if (myInput.MyTraceSource != null)
                    myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Exit, DateTime.Now.ToLongTimeString() + ": MergeDatabases ended");
            }
        }

        public void MakeDatabasePrimary()
        {
            if (myInput.MyTraceSource != null)
                myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Enter, DateTime.Now.ToShortDateString() + " " + DateTime.Now.ToShortTimeString() + ": MakeDatabasePrimary stared");
            var con = new SqlConnection(myInput.SQLConnectionString);
            con.Open();
            var isPrimaryCommand = con.CreateCommand();
            var trans = con.BeginTransaction("primary");
            isPrimaryCommand.Connection = con;
            isPrimaryCommand.Transaction = trans;
            try
            {
                isPrimaryCommand.CommandText = "UPDATE " + RoadNetworkDBManagerInput.MasterTableName + " SET isPrimary = 0  WHERE isPrimary = 1";
                isPrimaryCommand.ExecuteNonQuery();
                isPrimaryCommand.CommandText = "UPDATE " + RoadNetworkDBManagerInput.MasterTableName + " SET isPrimary = 1  WHERE associatedTableName = '" + myInput.DatabaseToModify + "'";
                isPrimaryCommand.ExecuteNonQuery();
                trans.Commit();
            }
            catch (Exception ex)
            {
                trans.Rollback("primary");
                if (myInput.MyTraceSource != null)
                {
                    string message = DateTime.Now.ToLongTimeString() + ": " + ex.ToString();
                    myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, message);
                }
            }
            finally
            {
                con.Close();
                if (myInput.MyTraceSource != null)
                    myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Exit, DateTime.Now.ToLongTimeString() + ": MakeDatabasePrimary ended");
            }
        }

        public void AddNewDatabase()
        {
            if (myInput.MyTraceSource != null)
                myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Enter, DateTime.Now.ToShortDateString() + " " + DateTime.Now.ToShortTimeString() + ": AddNewDatabase stared");
            string msg = ValidateAddNewDatabase();
            if (msg == string.Empty)
            {
                myWorker.RunWorkerAsync(myInput);
                if (myInput.MyTraceSource != null)
                    myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Exit, DateTime.Now.ToLongTimeString() + ": AddNewDatabase ended and left the import engine running.");
            }
            else
            {
                if (myInput.MyTraceSource != null)
                    myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Exit, DateTime.Now.ToLongTimeString() + ": AddNewDatabase ended with validation error: " + msg);
                throw new Exception(msg);
            }
        }

        private void myWorker_DoWork(object sender, DoWorkEventArgs e)
        {
            ShapeFileImporterOutput shapeFileImporterOutput = null;
            BackgroundWorker me = (BackgroundWorker)sender;
            ShapeFileImporter importer = null;
            var input = (RoadNetworkDBManagerInput)(e.Argument);
            switch (input.ImporterInput.MyDataProvider)
            {
                case DataProvider.Navteq:
                    importer = new NavteqImporter2();
                    break;
                case DataProvider.TeleAtlas:
                    // importer = new TeleAtlasImporter();
                    break;
            }
            if (input.ImporterInput.MyDataProvider == DataProvider.Navteq)
            {
                //  create the table for import and a record in master table
                var con = new SqlConnection(input.SQLConnectionString);
                con.Open();
                var addCommand = con.CreateCommand();
                var trans = con.BeginTransaction("add");
                addCommand.Connection = con;
                addCommand.Transaction = trans;
                try
                {
                    addCommand.CommandText = "IF OBJECT_ID('" + input.ImporterInput.RoadNetworkDatabaseName + "_Streets', 'U') IS NOT NULL DROP TABLE " + input.ImporterInput.RoadNetworkDatabaseName + "_Streets";
                    addCommand.ExecuteNonQuery();
                    addCommand.CommandText = "IF OBJECT_ID('" + input.ImporterInput.RoadNetworkDatabaseName + "_CDMS', 'U') IS NOT NULL DROP TABLE " + input.ImporterInput.RoadNetworkDatabaseName + "_CDMS";
                    addCommand.ExecuteNonQuery();
                    addCommand.CommandText = "IF OBJECT_ID('" + input.ImporterInput.RoadNetworkDatabaseName + "_RDMS', 'U') IS NOT NULL DROP TABLE " + input.ImporterInput.RoadNetworkDatabaseName + "_RDMS";
                    addCommand.ExecuteNonQuery();
                    addCommand.CommandText = "INSERT INTO " + RoadNetworkDBManagerInput.MasterTableName +
                      "(associatedTableName,isPrimary,[description],provider,providerVersion,dateAdded,recordCount,isErr)" +
                      " VALUES ('" + input.ImporterInput.RoadNetworkDatabaseName + "',0,'" + input.ImporterInput.DataDescription + "','" + input.ImporterInput.MyDataProvider +
                      "','" + input.ImporterInput.DataYear + " " + input.ImporterInput.DataMonth + "','" + DateTime.Now + "',0,-1)";
                    addCommand.ExecuteNonQuery();
                    trans.Commit();
                }
                catch (Exception ex)
                {
                    trans.Rollback("add");
                    if (input.MyTraceSource != null)
                    {
                        string message = DateTime.Now.ToLongTimeString() + ": " + ex.ToString();
                        input.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, message);
                    }
                }
                finally { con.Close(); }
                shapeFileImporterOutput = importer.SaveToSQL(me, input);
            }
            else shapeFileImporterOutput = new ShapeFileImporterOutput();
            shapeFileImporterOutput.Cancelled = me.CancellationPending;
            importer = null;
            e.Result = shapeFileImporterOutput;
        }

        private void myWorker_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            ImportProgressChanged(this, new RoadNetworkDBManagerEventArg(false, e.ProgressPercentage, "Importing " + e.UserState));
        }

        private void myWorker_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            var o = (ShapeFileImporterOutput)(e.Result);
            string statusMsg = "Import ";
            if (!o.Cancelled) statusMsg += "completed. ";
            else statusMsg += "cancelled. ";
            statusMsg += o.ProcessedFilesCount + " file(s) (" + o.ProcessedInsertCount + " records) processed.";
            // update the record count and isPrimary flag in master table
            var con = new SqlConnection(myInput.SQLConnectionString);
            con.Open();
            var finishCommand = con.CreateCommand();
            var trans = con.BeginTransaction("importfinish");
            finishCommand.Connection = con;
            finishCommand.Transaction = trans;
            try
            {
                finishCommand.CommandText =
                  "UPDATE " + RoadNetworkDBManagerInput.MasterTableName + " SET recordCount = " + o.ProcessedInsertCount + "  WHERE associatedTableName = '" + myInput.ImporterInput.RoadNetworkDatabaseName + "'";
                finishCommand.ExecuteNonQuery();
                if (!o.Cancelled)
                {
                    finishCommand.CommandText =
                      "UPDATE " + RoadNetworkDBManagerInput.MasterTableName + " SET isErr = " + o.ErrorCount + "  WHERE associatedTableName = '" + myInput.ImporterInput.RoadNetworkDatabaseName + "'";
                    finishCommand.ExecuteNonQuery();
                    if (myInput.ImporterInput.SetAsPrimary)
                    {
                        finishCommand.CommandText = "UPDATE " + RoadNetworkDBManagerInput.MasterTableName + " SET isPrimary = 0  WHERE isPrimary = 1";
                        finishCommand.ExecuteNonQuery();
                        finishCommand.CommandText =
                          "UPDATE " + RoadNetworkDBManagerInput.MasterTableName + " SET isPrimary = 1  WHERE associatedTableName = '" + myInput.ImporterInput.RoadNetworkDatabaseName + "'";
                        finishCommand.ExecuteNonQuery();
                    }
                }
                trans.Commit();
            }
            catch (Exception ex)
            {
                trans.Rollback("importfinish");
                if (myInput.MyTraceSource != null)
                {
                    string message = DateTime.Now.ToLongTimeString() + ": " + ex.ToString();
                    myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, message);
                    throw new Exception("Error while updating master table after import.", ex);
                }
            }
            finally
            {
                con.Close();
            }
            myInput.MyTraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completed, DateTime.Now.ToLongTimeString() + ": ShapeFileImport Worker ended.");
            ImportProgressChanged(this, new RoadNetworkDBManagerEventArg(true, 100, statusMsg));
        }

        public void CancelAddNewDatabase()
        {
            if (myInput.MyTraceSource != null)
                myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Enter, DateTime.Now.ToShortDateString() + " " + DateTime.Now.ToShortTimeString() + ": CancelAddNewDatabase reqested");
            myWorker.CancelAsync();
        }

        public string ValidateAddNewDatabase()
        {
            if (myInput.MyTraceSource != null)
                myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Enter, DateTime.Now.ToShortDateString() + " " + DateTime.Now.ToShortTimeString() + ": ValidateAddNewDatabase stared");
            string msg = string.Empty;
            try
            {
                if (cachedNameList != null) ListAvailableDatabaseNames();
                if (cachedNameList.Contains(myInput.ImporterInput.RoadNetworkDatabaseName)) msg = "Name is not uniqe.";
                else if (myInput.ImporterInput.MyDataProvider == DataProvider.Empty) msg = "Data Provider is not specified.";
                else if (!System.IO.Directory.Exists(myInput.ImporterInput.RootDirectory)) msg = "Directory not exists.";
                else if (this.IsBusy) msg = "Another process/worker is still busy and needs to be canceled first.";
                else if (myInput.ImporterInput.RoadNetworkDatabaseName.Contains("-") ||
                          myInput.ImporterInput.RoadNetworkDatabaseName.Contains(",") ||
                          myInput.ImporterInput.RoadNetworkDatabaseName.Contains(".") ||
                          myInput.ImporterInput.RoadNetworkDatabaseName.Contains(":") ||
                          myInput.ImporterInput.RoadNetworkDatabaseName.Contains(";") ||
                          myInput.ImporterInput.RoadNetworkDatabaseName.Contains("|") ||
                          myInput.ImporterInput.RoadNetworkDatabaseName.Contains("\\") ||
                          myInput.ImporterInput.RoadNetworkDatabaseName.Contains("/") ||
                          myInput.ImporterInput.RoadNetworkDatabaseName.Contains("{") ||
                          myInput.ImporterInput.RoadNetworkDatabaseName.Contains("}") ||
                          myInput.ImporterInput.RoadNetworkDatabaseName.Contains("[") ||
                          myInput.ImporterInput.RoadNetworkDatabaseName.Contains("]") ||
                          myInput.ImporterInput.RoadNetworkDatabaseName.Contains("(") ||
                          myInput.ImporterInput.RoadNetworkDatabaseName.Contains(")") ||
                          myInput.ImporterInput.RoadNetworkDatabaseName.Contains("!") ||
                          myInput.ImporterInput.RoadNetworkDatabaseName.Contains("@") ||
                          myInput.ImporterInput.RoadNetworkDatabaseName.Contains("'") ||
                          myInput.ImporterInput.RoadNetworkDatabaseName.Contains("\"")) msg = "The specified name is not a valid SQL Object Name.";
            }
            catch (Exception ex)
            {
                msg = ex.Message;
                if (myInput.MyTraceSource != null)
                    myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
            }
            if (myInput.MyTraceSource != null)
                myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Exit, DateTime.Now.ToLongTimeString() + ": ValidateAddNewDatabase ended");
            return msg;
        }

        public List<string> ListAvailableDatabaseNames()
        {
            if (myInput.MyTraceSource != null)
                myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Enter, DateTime.Now.ToShortDateString() + " " + DateTime.Now.ToShortTimeString() + ": ListAvailableDatabaseNames stared");
            if (cachedNameList != null) cachedNameList.Clear();
            else cachedNameList = new List<string>(5);
            var con = new SqlConnection(myInput.SQLConnectionString);
            try
            {
                con.Open();
                var select = con.CreateCommand();
                select.CommandText = "select associatedTableName from " + RoadNetworkDBManagerInput.MasterTableName;
                var reader = select.ExecuteReader();
                while (reader.Read()) cachedNameList.Add(reader.GetString(0));
                reader.Close();
                con.Close();
                if (myInput.MyTraceSource != null)
                    myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Exit, DateTime.Now.ToLongTimeString() + ": ListAvailableDatabaseNames ended");
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return cachedNameList;
        }

        public bool IsDatabasePrepered()
        {
            bool ret = false;
            var con = new SqlConnection(myInput.SQLConnectionString);
            try
            {
                con.Open();
            }
            catch { }
            try
            {
                if (con.State == ConnectionState.Open)
                {
                    var checkCommand = con.CreateCommand();
                    checkCommand.Connection = con;
                    checkCommand.CommandText = "SELECT [associatedTableName],[isPrimary],[description],[provider],[providerVersion],[dateAdded],[recordCount],[isErr] FROM " + RoadNetworkDBManagerInput.MasterTableName + " WHERE [isPrimary] = 1 and [isErr] = 0";
                    checkCommand.ExecuteNonQuery();
                }
                ret = true;
            }
            catch { }
            finally
            {
                if (con.State == ConnectionState.Open) con.Close();
                if (myInput.MyTraceSource != null)
                    myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Exit, DateTime.Now.ToShortDateString() + " " + DateTime.Now.ToShortTimeString() + ": IsDatabasePrepered ended. result: " + ret);
            }
            return ret;
        }

        public string SetupDatabase()
        {
            string ret = "Setup Complete", sql = string.Empty;
            var con = new SqlConnection(myInput.SQLConnectionString);
            SqlTransaction trans = null;
            try
            {
                // RoadNetwork Init
                if (!IsDatabasePrepered())
                {
                    con.Open();
                    trans = con.BeginTransaction("setup");
                    var setupCommand = con.CreateCommand();
                    setupCommand.Connection = con;
                    setupCommand.Transaction = trans;
                    setupCommand.CommandText = "IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('[" + RoadNetworkDBManagerInput.MasterTableName + "]') AND name = 'PK_" + RoadNetworkDBManagerInput.MasterTableName + "') DROP index " + RoadNetworkDBManagerInput.MasterTableName + ".PK_" + RoadNetworkDBManagerInput.MasterTableName;
                    setupCommand.ExecuteNonQuery();
                    setupCommand.CommandText = "IF OBJECT_ID('" + RoadNetworkDBManagerInput.MasterTableName + "', 'U') IS NOT NULL DROP TABLE " + RoadNetworkDBManagerInput.MasterTableName;
                    setupCommand.ExecuteNonQuery();
                    setupCommand.CommandText =
                        "CREATE TABLE [" + RoadNetworkDBManagerInput.MasterTableName + "](" + Environment.NewLine +
                        "[associatedTableName] [nvarchar](50) NOT NULL,[isPrimary] [bit] NOT NULL,[isErr] [int] NOT NULL," + Environment.NewLine +
                        "[description] [nvarchar](max) NULL,[provider] [nvarchar](50) NOT NULL," + Environment.NewLine +
                        "[providerVersion] [nvarchar](50) NULL,[dateAdded] [datetime] NOT NULL," + Environment.NewLine +
                        "[recordCount] [int] NULL,CONSTRAINT [PK_" + RoadNetworkDBManagerInput.MasterTableName + "] PRIMARY KEY CLUSTERED" + Environment.NewLine +
                        "([associatedTableName] ASC)) ON [PRIMARY]";
                    setupCommand.ExecuteNonQuery();
                    trans.Commit();
                    con.Close();
                }
            }
            catch (Exception ex)
            {
                if (con.State == ConnectionState.Open) trans.Rollback();
                ret = ex.Message;
            }
            finally
            {
                if (con.State == ConnectionState.Open) con.Close();
                if (myInput.MyTraceSource != null)
                    myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Exit, DateTime.Now.ToLongTimeString() + ": SetupDatabase Road DB ended. result: " + ret);
            }

            try
            {
                // User Tables Init  
                con = new SqlConnection(myInput.GetConnectionString(myInput.UserTableDBName));
                con.Open();
                con.Close();

                // WebApp Init
                con = new SqlConnection(myInput.GetConnectionString(myInput.WebAppDBName));
                con.Open();
                var webappCommand = con.CreateCommand();
                webappCommand.Connection = con;
                sql = global::Tamu.GeoInnovation.ShortestPath.RoadNetworkData.Properties.Resources.WebAppSQL;
                sql = sql.Replace("[DBNAME]", myInput.WebAppDBName);
                webappCommand.CommandText = sql;
                webappCommand.ExecuteNonQuery();
                con.Close();
            }
            catch (Exception ex)
            {
                ret = ex.Message;
            }
            finally
            {
                if (con.State == ConnectionState.Open) con.Close();
                if (myInput.MyTraceSource != null)
                    myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)FunctionEvents.Exit, DateTime.Now.ToLongTimeString() + ": SetupDatabase WebApp DB ended. result: " + ret);
            }

            return ret;
        }
    }
}