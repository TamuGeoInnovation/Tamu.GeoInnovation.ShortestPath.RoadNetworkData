using Microsoft.SqlServer.Types;
using Reimers.Esri;
using System;
using System.ComponentModel;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using USC.GISResearchLab.Common.Databases.Odbc;
using USC.GISResearchLab.Common.Databases.TypeConverters;
using USC.GISResearchLab.Common.Diagnostics.TraceEvents;
using USC.GISResearchLab.Common.Utils.Databases.TableDefinitions;

namespace USC.GISResearchLab.ShortestPath.RoadNetworkData
{
	public class NavteqImporter2 : ShapeFileImporter
	{
		public NavteqImporter2()
			: base() { }

		public override ShapeFileImporterOutput SaveToSQL(BackgroundWorker MyWorker, RoadNetworkDBManagerInput myInput)
		{
			var ret = new ShapeFileImporterOutput();
			SqlGeography shape = null;
			Shapefile shapeFile = null;
			string msg = string.Empty, pmsg = string.Empty;
			int mcount = 0;
			double dbfTimeRatio = 1.0, AdvancedPercentage = 0.0, dbfRowCount = 0.0;
			IAsyncResult asyncIndex = null, asyncImport = null;
			SqlConnection con = new SqlConnection(), conIndex = new SqlConnection();
			OdbcDataReader dbfDataReader = null;
			OdbcCommand ocmd = null;
			OdbcConnection conn = null;
			SqlCommand cmd = null, cmdIndex = null;
			OdbcSchemaManager odbcMan = null;
			TableColumn[] schema = null;
			DateTime start = DateTime.Now;
			string status = string.Empty;
			try
			{
				if (myInput.MyTraceSource != null)
				{
					myInput.MyTraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Started, DateTime.Now.ToLongTimeString() + ": NavteqImporter SaveToSQL Start");
					myInput.MyTraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Running, "NavteqImporter SaveToSQL Configuration: " + myInput.ToString());
				}
				string[] StreetsFiles = null, RDMSFiles = null, CDMSFiles = null;
				bool StreetTableCreated = false, RDMSTableCreated = false, CDMSTableCreated = false;
				double streetLen = 0;

				string cmdInsertQuery = string.Empty, cmdInsert = string.Empty, columnString = string.Empty, Linedata = string.Empty;

				MyWorker.ReportProgress(0, "(Preparing data files) ...");

				StreetsFiles = Directory.GetFiles(myInput.ImporterInput.RootDirectory, "Streets.shp", SearchOption.AllDirectories);
				RDMSFiles = Directory.GetFiles(myInput.ImporterInput.RootDirectory, "Rdms.dbf", SearchOption.AllDirectories);
				CDMSFiles = Directory.GetFiles(myInput.ImporterInput.RootDirectory, "Cdms.dbf", SearchOption.AllDirectories);

				if (myInput.SQLVersion == SQLVersionEnum.SQLServer2008) dbfTimeRatio = 5.879385622;
				totalFilesBytes = 0;
				foreach (string f in StreetsFiles) totalFilesBytes += (new FileInfo(f)).Length;
				foreach (string f in RDMSFiles) totalFilesBytes += (new FileInfo(f)).Length * dbfTimeRatio;
				foreach (string f in CDMSFiles) totalFilesBytes += (new FileInfo(f)).Length * dbfTimeRatio;

				con.ConnectionString = myInput.SQLConnectionString;
				con.Open();

				schema = null;

				foreach (string StreetsFile in StreetsFiles)
				{
					#region Streets File

					currentFilePercent = (new FileInfo(StreetsFile)).Length * 100 / totalFilesBytes;
					try
					{
						streetLen = 0;
						msg = string.Empty;
						pmsg = string.Empty;
						mcount = 0;
						shapeFile = new Shapefile(StreetsFile);
						shapeFile.DoCopyDBFInStreamMode = false;

						if (schema == null)
						{
							schema = shapeFile.GetDBFSchema();
							start = DateTime.Now;
						}

						cmdInsertQuery = "INSERT INTO " + myInput.ImporterInput.RoadNetworkDatabaseName + "_Streets VALUES (";
						columnString = string.Empty;

						for (int i = 0; i < schema.Length; i++)
						{
							columnString += "[" + schema[i].Name + "] ";
							switch (schema[i].DatabaseSuperDataType)
							{
								case DatabaseSuperDataType.Double:
								case DatabaseSuperDataType.Float:
								case DatabaseSuperDataType.Decimal: columnString += "NUMERIC(16,6) NOT NULL, ";
									break;
								case DatabaseSuperDataType.UInt24:
								case DatabaseSuperDataType.UInt32:
								case DatabaseSuperDataType.UInt16:
								case DatabaseSuperDataType.Int24:
								case DatabaseSuperDataType.Int32:
                                case DatabaseSuperDataType.BigInt:
                                case DatabaseSuperDataType.Numeric:
								case DatabaseSuperDataType.Int16: columnString += "INT NOT NULL, ";
                                    break;
								case DatabaseSuperDataType.Boolean: columnString += "BOOL, ";
									break;
                                case DatabaseSuperDataType.String:
                                case DatabaseSuperDataType.Char:
								case DatabaseSuperDataType.NVarChar:
								case DatabaseSuperDataType.VarChar:
								case DatabaseSuperDataType.NText:
								case DatabaseSuperDataType.Text: columnString += "VARCHAR(255), ";
									break;
								default:
									throw new NotSupportedException("Not supported datatype in shapefile: " + StreetsFile + " -> " + schema[i].DatabaseSuperDataType);
							}
						}

						if (myInput.SQLVersion == SQLVersionEnum.SQLServer2008)
							columnString += "[Shape] geography, [Len] NUMERIC(16,6) NOT NULL";
						else
							columnString += "[LineData] VARCHAR(MAX), [Len] NUMERIC(16,6) NOT NULL";
						if (!StreetTableCreated)
						{
							if (con != null && con.State != ConnectionState.Open) con.Open();
							cmd = con.CreateCommand();
							cmd.CommandTimeout = 300;
							cmd.CommandText = "CREATE TABLE " + myInput.ImporterInput.RoadNetworkDatabaseName + "_Streets (" + columnString + ",CONSTRAINT [PK_" +
								  myInput.ImporterInput.RoadNetworkDatabaseName + "_Streets] PRIMARY KEY CLUSTERED" + Environment.NewLine + "([LINK_ID] ASC)) ON [PRIMARY]";
							cmd.CommandType = CommandType.Text;
							cmd.ExecuteNonQuery();

							StreetTableCreated = true;
							if (myInput.MyTraceSource != null)
								myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, DateTime.Now.ToLongTimeString() + ": Streets Table Created");
						}
						if (myInput.MyTraceSource != null)
							myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, DateTime.Now.ToLongTimeString() + ": Streets running (" + StreetsFile + ")");

						if (MyWorker.CancellationPending) break;

						for (var rec = shapeFile.NextFeature(); rec != null; rec = shapeFile.NextFeature())
						{
							try
							{
								if (MyWorker.CancellationPending) break;

								AdvancedPercentage = rec.SteamedBytesRatio;

								for (int i = 0; i < rec.DataArray.Length; i++)
								{
                                    if ((schema[i].DatabaseSuperDataType == DatabaseSuperDataType.String) ||
                                        (schema[i].DatabaseSuperDataType == DatabaseSuperDataType.Char) ||
										(schema[i].DatabaseSuperDataType == DatabaseSuperDataType.NVarChar) ||
										(schema[i].DatabaseSuperDataType == DatabaseSuperDataType.VarChar) ||
										(schema[i].DatabaseSuperDataType == DatabaseSuperDataType.NText) ||
										(schema[i].DatabaseSuperDataType == DatabaseSuperDataType.Text))
										cmdInsert += "'" + rec.DataArray[i].ToString().Replace("'", "''") + "',";
									else
									{
										if (rec.DataArray[i] is DBNull)
											cmdInsert += "0,";
										else
											cmdInsert += rec.DataArray[i].ToString() + ",";
									}
								}

								shape = rec.Shape.ToUnionSqlGeography("");
								streetLen = shape.STLength().Value / 1609.344;
								shape = shape.Reduce(3.0);
								Linedata = shape.STAsText().ToSqlString().Value;

								if (con != null && con.State != ConnectionState.Open) con.Open();
								cmdInsert += "@shape," + streetLen + ")";

								if ((asyncImport != null) && (cmd != null))
								{
									cmd.EndExecuteNonQuery(asyncImport);
									asyncImport = null;
								}

								cmd = con.CreateCommand();
								cmd.CommandTimeout = 300;
								cmd.CommandText = cmdInsertQuery + cmdInsert;
								if (myInput.SQLVersion == SQLVersionEnum.SQLServer2008)
								{
									cmd.Parameters.AddWithValue("@shape", shape);
									cmd.Parameters["@shape"].UdtTypeName = "geography";
								}
								else
								{
									cmd.Parameters.AddWithValue("@shape", Linedata);
									cmd.Parameters["@shape"].UdtTypeName = "VARCHAR(255)";
								}
								cmd.CommandType = CommandType.Text;
								asyncImport = cmd.BeginExecuteNonQuery();
								ret.ProcessedInsertCount++;
							}
							catch (Exception ex)
							{
								msg = DateTime.Now.ToLongTimeString() + ": NavteqImporter SaveToSQL: An error occured during shapefile record process: " + ex.ToString();
								if (myInput.MyTraceSource != null)
									myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
								if (pmsg == ex.ToString()) mcount++;
								else
								{
									pmsg = ex.ToString();
									mcount = 0;
								}
								ret.ErrorCount++;
								if (mcount > 4) throw new Exception("Repeated error found in this shapefile. Breaking the loop...");
							}
							finally
							{
								completedPercent += AdvancedPercentage * currentFilePercent;
								status = StreetsFile.Substring(myInput.ImporterInput.RootDirectory.Length, StreetsFile.Length - myInput.ImporterInput.RootDirectory.Length);
								status += Environment.NewLine + NavteqImporter2.GetRemainingTime(start, completedPercent);
								MyWorker.ReportProgress(Convert.ToInt32(completedPercent), status);
								cmdInsert = string.Empty;
							}
						}

						if ((asyncImport != null) && (cmd != null))
						{
							cmd.EndExecuteNonQuery(asyncImport);
							asyncImport = null;
						}
						ret.ProcessedFilesCount++;
					}
					catch (Exception ex)
					{
						msg = DateTime.Now.ToLongTimeString() + ": NavteqImporter SaveToSQL: An error occured during shape file process: " + ex.ToString();
						if (myInput.MyTraceSource != null)
							myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
						ret.ErrorCount++;
					}
					finally
					{
						shapeFile.CloseStream();
						cmdInsert = string.Empty;
					}

					#endregion
				}

				// streets indexing stage (acync)				
				// http://msdn.microsoft.com/en-us/library/bb934196.aspx

				conIndex.ConnectionString = myInput.SQLConnectionString;
				conIndex.Open();
				cmdIndex = conIndex.CreateCommand();
				cmdIndex.CommandTimeout = 0;
				cmdIndex.CommandType = CommandType.Text;
				if (myInput.SQLVersion == SQLVersionEnum.SQLServer2005)
				{
					cmdIndex.CommandText = "CREATE NONCLUSTERED INDEX IX_" + myInput.ImporterInput.RoadNetworkDatabaseName + "_Streets ON dbo." + myInput.ImporterInput.RoadNetworkDatabaseName +
						  "_Streets" + Environment.NewLine + "(LINK_ID) INCLUDE (SPEED_CAT, FROMLONG, FROMLAT, TOLONG, TOLAT) " + Environment.NewLine +
						  "WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]";
				}
				else if (myInput.SQLVersion == SQLVersionEnum.SQLServer2008)
				{
                    cmdIndex.CommandText = "CREATE SPATIAL INDEX SIX1_" + myInput.ImporterInput.RoadNetworkDatabaseName + "_Streets ON " + myInput.ImporterInput.RoadNetworkDatabaseName + "_Streets(shape);" + Environment.NewLine +
                        "CREATE SPATIAL INDEX SIX2_" + myInput.ImporterInput.RoadNetworkDatabaseName + "_Streets ON " + myInput.ImporterInput.RoadNetworkDatabaseName + "_Streets(Shape) USING GEOGRAPHY_GRID " +
                        "WITH( GRIDS = ( LEVEL_1 = HIGH, LEVEL_2 = HIGH, LEVEL_3 = HIGH, LEVEL_4 = HIGH), CELLS_PER_OBJECT = 1024, STATISTICS_NORECOMPUTE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);";
				}
				if (!MyWorker.CancellationPending) asyncIndex = cmdIndex.BeginExecuteNonQuery();

				schema = null;
				asyncImport = null;

				foreach (string CDMSFile in CDMSFiles)
				{
					#region CDMS File

					currentFilePercent = (new FileInfo(CDMSFile)).Length * dbfTimeRatio * 100 / totalFilesBytes;
					try
					{
						msg = string.Empty;
						pmsg = string.Empty;

						if (MyWorker.CancellationPending) break;

						if (schema == null)
						{
							odbcMan = new OdbcSchemaManager("DBQ=" + Path.GetDirectoryName(CDMSFile) + "\\;Driver={Microsoft dBase Driver (*.dbf)};DriverId=277;FIL=dBase4.0");
							schema = odbcMan.GetColumns(Path.GetFileName(CDMSFile));
						}
						cmdInsertQuery = "INSERT INTO " + myInput.ImporterInput.RoadNetworkDatabaseName + "_CDMS ( ";
						columnString = string.Empty;

						for (int i = 0; i < schema.Length; i++)
						{
							cmdInsertQuery += "[";
							columnString += "[" + schema[i].Name + "] ";
							cmdInsertQuery += schema[i].Name + "], ";
							switch (schema[i].DatabaseSuperDataType)
							{
								case DatabaseSuperDataType.Double:
								case DatabaseSuperDataType.Float:
								case DatabaseSuperDataType.Decimal: columnString += "NUMERIC(16,6) NOT NULL, ";
									break;
								case DatabaseSuperDataType.UInt24:
								case DatabaseSuperDataType.UInt32:
								case DatabaseSuperDataType.UInt16:
								case DatabaseSuperDataType.Int24:
								case DatabaseSuperDataType.Int32:
                                case DatabaseSuperDataType.BigInt:
                                case DatabaseSuperDataType.Numeric:
								case DatabaseSuperDataType.Int16: columnString += "INT NOT NULL, ";
									break;

								case DatabaseSuperDataType.Boolean: columnString += "BOOL, ";
									break;
								case DatabaseSuperDataType.String:
								case DatabaseSuperDataType.NVarChar:
								case DatabaseSuperDataType.VarChar:
                                case DatabaseSuperDataType.NText:
                                case DatabaseSuperDataType.Char:
								case DatabaseSuperDataType.Text: columnString += "VARCHAR(255), ";
									break;
								default:
									throw new NotSupportedException("Not supported datatype in dbf file: " + CDMSFile + " -> " + schema[i].DatabaseSuperDataType);
							}
						}
						columnString = columnString.Remove(columnString.Length - 2, 2);
						cmdInsertQuery = cmdInsertQuery.Remove(cmdInsertQuery.Length - 2, 2);
						cmdInsertQuery += ") VALUES (";

						if (con != null && con.State == ConnectionState.Closed) con.Open();
						if (!CDMSTableCreated)
						{
							cmd = con.CreateCommand();
							cmd.CommandTimeout = 300;
							cmd.CommandText = "CREATE TABLE " + myInput.ImporterInput.RoadNetworkDatabaseName + "_CDMS (" + columnString + ",CONSTRAINT [PK_" +
							  myInput.ImporterInput.RoadNetworkDatabaseName + "_CDMS] PRIMARY KEY CLUSTERED" + Environment.NewLine +
							  "(COND_ID ASC)) ON [PRIMARY]";
							cmd.CommandType = CommandType.Text;
							cmd.ExecuteNonQuery();
							CDMSTableCreated = true;
							if (myInput.MyTraceSource != null)
								myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, DateTime.Now.ToLongTimeString() + ": CDMS Table Created");
						}
						if (myInput.MyTraceSource != null)
							myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, DateTime.Now.ToLongTimeString() + ": CDMS running (" + CDMSFile + ")");

						conn = new OdbcConnection("DBQ=" + Path.GetDirectoryName(CDMSFile) + "\\;Driver={Microsoft dBASE Driver (*.dbf)};DriverId=277;FIL=dBase4.0");

						conn.Open();
						ocmd = conn.CreateCommand();
						ocmd.CommandTimeout = 300;
						ocmd.CommandText = "SELECT count(*) FROM [" + Path.GetFileName(CDMSFile) + "]";
						dbfRowCount = Convert.ToDouble(ocmd.ExecuteScalar());

						ocmd = conn.CreateCommand();
						ocmd.CommandTimeout = 0;
						ocmd.CommandText = "SELECT * FROM [" + Path.GetFileName(CDMSFile) + "]";
						dbfDataReader = ocmd.ExecuteReader();

						while (dbfDataReader.Read())
						{
							try
							{
								if (MyWorker.CancellationPending) break;
								cmdInsert = string.Empty;

								for (int i = 0; i < dbfDataReader.FieldCount; i++)
								{
									if ((schema[i].DatabaseSuperDataType == DatabaseSuperDataType.String) ||
										(schema[i].DatabaseSuperDataType == DatabaseSuperDataType.NVarChar) ||
                                        (schema[i].DatabaseSuperDataType == DatabaseSuperDataType.VarChar) ||
                                        (schema[i].DatabaseSuperDataType == DatabaseSuperDataType.Char) ||
										(schema[i].DatabaseSuperDataType == DatabaseSuperDataType.NText) ||
										(schema[i].DatabaseSuperDataType == DatabaseSuperDataType.Text))
										cmdInsert += "'" + dbfDataReader.GetValue(i).ToString().Replace("'", "''") + "',";
									else
									{
										if (dbfDataReader.GetValue(i) is DBNull)
											cmdInsert += "0,";
										else
											cmdInsert += dbfDataReader.GetValue(i).ToString() + ",";
									}
								}
								cmdInsert = cmdInsert.Substring(0, cmdInsert.Length - 1);
								cmdInsert += ")";
								if (con != null && con.State != ConnectionState.Open) con.Open();

								if ((asyncImport != null) && (cmd != null))
								{
									cmd.EndExecuteNonQuery(asyncImport);
									asyncImport = null;
								}
								cmd = con.CreateCommand();
								cmd.CommandTimeout = 300;
								cmd.CommandText = cmdInsertQuery + " " + cmdInsert;
								cmd.CommandType = CommandType.Text;
								asyncImport = cmd.BeginExecuteNonQuery();
								ret.ProcessedInsertCount++;
							}
							catch (Exception ex)
							{
								msg = DateTime.Now.ToLongTimeString() + ": NavteqImporter SaveToSQL: An error occured during CDMS record process: " + ex.ToString();
								if (myInput.MyTraceSource != null)
									myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
								if (pmsg == ex.ToString()) mcount++;
								else
								{
									pmsg = ex.ToString();
									mcount = 0;
								}
								ret.ErrorCount++;
								if (mcount > 4) throw new Exception("Repeated error found in this DBF file. Breaking the loop...");
							}
							finally
							{
								completedPercent += 1 * currentFilePercent / dbfRowCount;
								status = CDMSFile.Substring(myInput.ImporterInput.RootDirectory.Length, CDMSFile.Length - myInput.ImporterInput.RootDirectory.Length);
								status += Environment.NewLine + NavteqImporter2.GetRemainingTime(start, completedPercent);
								MyWorker.ReportProgress(Convert.ToInt32(completedPercent), status);
							}
						}

						if ((asyncImport != null) && (cmd != null))
						{
							cmd.EndExecuteNonQuery(asyncImport);
							asyncImport = null;
						}
						ret.ProcessedFilesCount++;
					}
					catch (Exception ex)
					{
						msg = DateTime.Now.ToLongTimeString() + ": NavteqImporter SaveToSQL: An error occured during CDMS process: " + ex.ToString();
						if (myInput.MyTraceSource != null)
							myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
						ret.ErrorCount++;
					}
					finally
					{
						if (dbfDataReader != null) dbfDataReader.Close();
						if (conn != null) conn.Close();
					}

					#endregion
				}

				schema = null;
				asyncImport = null;

				foreach (string RDMSFile in RDMSFiles)
				{
					#region RDMS File

					currentFilePercent = (new FileInfo(RDMSFile)).Length * dbfTimeRatio * 100 / totalFilesBytes;
					try
					{
						msg = string.Empty;
						pmsg = string.Empty;
						mcount = 0;

						if (MyWorker.CancellationPending) break;

						if (schema == null)
						{
							odbcMan = new OdbcSchemaManager("DBQ=" + Path.GetDirectoryName(RDMSFile) + "\\;Driver={Microsoft dBase Driver (*.dbf)};DriverId=277;FIL=dBase4.0");
							schema = odbcMan.GetColumns(Path.GetFileName(RDMSFile));
						}
						cmdInsertQuery = "INSERT INTO " + myInput.ImporterInput.RoadNetworkDatabaseName + "_RDMS ( "; columnString = string.Empty;

						for (int i = 0; i < schema.Length; i++)
						{
							cmdInsertQuery += "[";
							columnString += "[" + schema[i].Name + "] ";
							cmdInsertQuery += schema[i].Name + "], ";
							switch (schema[i].DatabaseSuperDataType)
							{
								case DatabaseSuperDataType.Double:
								case DatabaseSuperDataType.Float:
								case DatabaseSuperDataType.Decimal: columnString += "NUMERIC(16,6) NOT NULL, ";
									break;
								case DatabaseSuperDataType.UInt24:
								case DatabaseSuperDataType.UInt32:
								case DatabaseSuperDataType.UInt16:
								case DatabaseSuperDataType.Int24:
								case DatabaseSuperDataType.Int32:
                                case DatabaseSuperDataType.BigInt:
                                case DatabaseSuperDataType.Numeric:
								case DatabaseSuperDataType.Int16: columnString += "INT NOT NULL, ";
									break;

								case DatabaseSuperDataType.Boolean: columnString += "BOOL, ";
									break;
								case DatabaseSuperDataType.String:
								case DatabaseSuperDataType.NVarChar:
								case DatabaseSuperDataType.VarChar:
                                case DatabaseSuperDataType.NText:
                                case DatabaseSuperDataType.Char:
								case DatabaseSuperDataType.Text: columnString += "VARCHAR(255), ";
									break;
								default:
									throw new NotSupportedException("Not supported datatype in dbf file: " + RDMSFile + " -> " + schema[i].DatabaseSuperDataType);
							}
						}
						columnString = columnString.Remove(columnString.Length - 2, 2);
						cmdInsertQuery = cmdInsertQuery.Remove(cmdInsertQuery.Length - 2, 2);
						cmdInsertQuery += ") VALUES (";

						if (!RDMSTableCreated)
						{
							if (con != null && con.State != ConnectionState.Open) con.Open();
							cmd = con.CreateCommand();
							cmd.CommandTimeout = 300;
							cmd.CommandText = "CREATE TABLE " + myInput.ImporterInput.RoadNetworkDatabaseName + "_RDMS (" + columnString + ",CONSTRAINT [PK_" +
							  myInput.ImporterInput.RoadNetworkDatabaseName + "_RDMS] PRIMARY KEY CLUSTERED" + Environment.NewLine +
							  "([COND_ID] ASC, [SEQ_NUMBER] ASC)) ON [PRIMARY]";
							cmd.CommandType = CommandType.Text;
							cmd.ExecuteNonQuery();
							RDMSTableCreated = true;
							if (myInput.MyTraceSource != null)
								myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, DateTime.Now.ToLongTimeString() + ": RDMS Table Created");
						}
						if (myInput.MyTraceSource != null)
							myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, DateTime.Now.ToLongTimeString() + ": RDMS running (" + RDMSFile + ")");

						conn = new OdbcConnection("DBQ=" + Path.GetDirectoryName(RDMSFile) + "\\;Driver={Microsoft dBASE Driver (*.dbf)};DriverId=277;FIL=dBase4.0");

						conn.Open();
						ocmd = conn.CreateCommand();
						ocmd.CommandTimeout = 300;
						ocmd.CommandText = "SELECT count(*) FROM [" + Path.GetFileName(RDMSFile) + "]";
						dbfRowCount = Convert.ToDouble(ocmd.ExecuteScalar());

						ocmd = conn.CreateCommand();
						ocmd.CommandTimeout = 0;
						ocmd.CommandText = "SELECT * FROM [" + Path.GetFileName(RDMSFile) + "]";
						dbfDataReader = ocmd.ExecuteReader();

						while (dbfDataReader.Read())
						{
							try
							{
								if (MyWorker.CancellationPending) break;
								cmdInsert = string.Empty;

								for (int i = 0; i < dbfDataReader.FieldCount; i++)
								{
									if ((schema[i].DatabaseSuperDataType == DatabaseSuperDataType.String) ||
										(schema[i].DatabaseSuperDataType == DatabaseSuperDataType.NVarChar) ||
                                        (schema[i].DatabaseSuperDataType == DatabaseSuperDataType.VarChar) ||
                                        (schema[i].DatabaseSuperDataType == DatabaseSuperDataType.Char) ||
										(schema[i].DatabaseSuperDataType == DatabaseSuperDataType.NText) ||
										(schema[i].DatabaseSuperDataType == DatabaseSuperDataType.Text))
										cmdInsert += "'" + dbfDataReader.GetValue(i).ToString().Replace("'", "''") + "',";
									else
									{
										if (dbfDataReader.GetValue(i) is DBNull)
											cmdInsert += "0,";
										else
											cmdInsert += dbfDataReader.GetValue(i).ToString() + ",";
									}
								}

								cmdInsert = cmdInsert.Substring(0, cmdInsert.Length - 1);
								cmdInsert += ")";
								if (con != null && con.State != ConnectionState.Open) con.Open();

								if ((asyncImport != null) && (cmd != null))
								{
									cmd.EndExecuteNonQuery(asyncImport);
									asyncImport = null;
								}
								cmd = con.CreateCommand();
								cmd.CommandTimeout = 300;
								cmd.CommandText = cmdInsertQuery + " " + cmdInsert;
								cmd.CommandType = CommandType.Text;
								asyncImport = cmd.BeginExecuteNonQuery();
								ret.ProcessedInsertCount++;
							}
							catch (Exception ex)
							{
								msg = DateTime.Now.ToLongTimeString() + ": NavteqImporter SaveToSQL: An error occured during RDMS record process: " + ex.ToString();
								if (myInput.MyTraceSource != null)
									myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
								ret.ErrorCount++;
							}
							finally
							{
								completedPercent += 1 * currentFilePercent / dbfRowCount;
								status = RDMSFile.Substring(myInput.ImporterInput.RootDirectory.Length, RDMSFile.Length - myInput.ImporterInput.RootDirectory.Length);
								status += Environment.NewLine + NavteqImporter2.GetRemainingTime(start, completedPercent);
								MyWorker.ReportProgress(Convert.ToInt32(completedPercent), status);
							}
						}

						if ((asyncImport != null) && (cmd != null))
						{
							cmd.EndExecuteNonQuery(asyncImport);
							asyncImport = null;
						}
						ret.ProcessedFilesCount++;
					}
					catch (Exception ex)
					{
						msg = "NavteqImporter SaveToSQL: An error occured during process: " + ex.ToString();
						if (myInput.MyTraceSource != null)
							myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
						if (pmsg == ex.ToString()) mcount++;
						else
						{
							pmsg = ex.ToString();
							mcount = 0;
						}
						ret.ErrorCount++;
						if (mcount > 4) throw new Exception("Repeated error found in this shape file. Breaking the loop...");
					}
					finally
					{
						if (dbfDataReader != null) dbfDataReader.Close();
						if (conn != null) conn.Close();
					}

					#endregion
				}

				// finishing the indexing...
				if (myInput.MyTraceSource != null)
					myInput.MyTraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completing,
					DateTime.Now.ToLongTimeString() + " Waiting for the index command to finish.");
				MyWorker.ReportProgress(99, "(Waiting for the indexing to finish) ...");
				if ((cmdIndex != null) && (asyncIndex != null)) cmdIndex.EndExecuteNonQuery(asyncIndex);
				conIndex.Close();
			}
			catch (Exception ex)
			{
				msg = DateTime.Now.ToLongTimeString() + ": NavteqImporter SaveToSQL: An error occured during process: " + ex.ToString();
				if (myInput.MyTraceSource != null)
					myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
				ret.ErrorCount++;
			}
			finally
			{
				con.Close();
				conIndex.Close();
				GC.Collect();
				GC.WaitForPendingFinalizers();
				if (myInput.MyTraceSource != null)
					myInput.MyTraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Completed,
					DateTime.Now.ToLongTimeString() + ": NavteqImporter SaveToSQL completed");
			}
			return ret;
		}

		public override ShapeFileImporterOutput SaveToDisk(BackgroundWorker MyWorker, RoadNetworkDBManagerInput myInput)
		{
			throw new NotImplementedException();
		}
	}
}