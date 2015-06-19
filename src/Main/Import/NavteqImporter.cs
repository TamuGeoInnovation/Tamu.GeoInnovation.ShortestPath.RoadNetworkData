using System;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.SqlServer.Types;
using USC.GISResearchLab.Common.Diagnostics.TraceEvents;
using USC.GISResearchLab.Common.Geographics.DistanceFunctions;
using USC.GISResearchLab.Common.Geographics.Units;
using USC.GISResearchLab.Common.ShapeLibs;

namespace USC.GISResearchLab.ShortestPath.RoadNetworkData
{
	public class NavteqImporter : ShapeFileImporter
	{
		public NavteqImporter()
			: base() { }

		public override ShapeFileImporterOutput SaveToSQL(BackgroundWorker MyWorker, RoadNetworkDBManagerInput myInput)
		{
			var ret = new ShapeFileImporterOutput();
			var shape = new SqlGeography();
			string msg = string.Empty, pmsg = string.Empty;
			int mcount = 0;
			double dbfTimeRatio = 1.0;
			IntPtr ptrSHP = IntPtr.Zero;
			IntPtr ptrDBF = IntPtr.Zero;
			var con = new SqlConnection();
			SqlCommand cmd = null;
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
				int nEntities = 0, length = 0, decimals = 0, fieldWidth = 0, fieldCount = 0, NoOfDBFRec = 0;

				StringBuilder strFieldName = null;
				double[] adfMin = null, adfMax = null, Xarr = null, Yarr = null;

				ShapeLib.SHPObject shpObj = null;
				IntPtr pshpObj = IntPtr.Zero;
				ShapeLib.ShapeType nShapeType = 0;
				ShapeLib.DBFFieldType fType;
				ShapeLib.DBFFieldType type;
				shape.STSrid = 4326;

				string cmdInsertQuery = string.Empty, cmdInsert = string.Empty, columnString = string.Empty, Linedata = string.Empty;

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

				foreach (string StreetsFile in StreetsFiles)
				{
					#region Streets File

					currentFilePercent = (new FileInfo(StreetsFile)).Length * 100 / totalFilesBytes;
					try
					{
						nEntities = 0;
						length = 0;
						decimals = 0;
						fieldWidth = 0;
						streetLen = 0;
						nShapeType = 0;
						msg = string.Empty;
						pmsg = string.Empty;
						mcount = 0;

						adfMin = new double[2];
						adfMax = new double[2];
						Xarr = null; Yarr = null;

						ptrSHP = IntPtr.Zero;
						ptrSHP = ShapeLib.SHPOpen(StreetsFile, "r+b");
						if ((Marshal.GetLastWin32Error() != 0) && (ptrSHP == IntPtr.Zero)) Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error(), Marshal.GetExceptionPointers());
						ShapeLib.SHPGetInfo(ptrSHP, ref nEntities, ref nShapeType, adfMin, adfMax);
						if ((Marshal.GetLastWin32Error() != 0) && (nShapeType == 0)) Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error(), Marshal.GetExceptionPointers());
						fType = ShapeLib.DBFFieldType.FTInvalid;

						ptrDBF = IntPtr.Zero;
						ptrDBF = ShapeLib.DBFOpen(StreetsFile, "r+b");
						if ((Marshal.GetLastWin32Error() != 0) && (ptrDBF == IntPtr.Zero))
							Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error(), Marshal.GetExceptionPointers());
						NoOfDBFRec = ShapeLib.DBFGetRecordCount(ptrDBF);
						if ((Marshal.GetLastWin32Error() != 0) && (NoOfDBFRec < 0)) Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error(), Marshal.GetExceptionPointers());

						strFieldName = new StringBuilder(String.Empty);
						cmdInsertQuery = "INSERT INTO " + myInput.ImporterInput.RoadNetworkDatabaseName + "_Streets VALUES (";
						columnString = string.Empty;
						fieldCount = ShapeLib.DBFGetFieldCount(ptrDBF);

						for (int i = 0; i < fieldCount; i++)
						{
							strFieldName.Append(string.Empty);
							// cmdInsertQuery += " [";
							columnString += "[";
							fType = ShapeLib.DBFFieldType.FTInvalid;
							fType = ShapeLib.DBFGetFieldInfo(ptrDBF, i, strFieldName, ref fieldWidth, ref decimals);
							if ((Marshal.GetLastWin32Error() != 0) && (fType == ShapeLib.DBFFieldType.FTInvalid))
								Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error(), Marshal.GetExceptionPointers());
							columnString += strFieldName + "] ";
							// cmdInsertQuery += strFieldName + "], ";
							switch (fType)
							{
								case ShapeLib.DBFFieldType.FTDouble: columnString += "NUMERIC(15,6) NOT NULL, ";
									break;
								case ShapeLib.DBFFieldType.FTInteger: columnString += "INT NOT NULL, ";
									break;
								case ShapeLib.DBFFieldType.FTLogical: columnString += "BOOL, ";
									break;
								case ShapeLib.DBFFieldType.FTString: columnString += "VARCHAR(255), ";
									break;
							}
						}

						if (myInput.SQLVersion == SQLVersionEnum.SQLServer2008)
							columnString += "[FROMLONG] NUMERIC(15,6) NOT NULL, [FROMLAT] NUMERIC(15,6) NOT NULL, [TOLONG] NUMERIC(15,6) NOT NULL, [TOLAT] NUMERIC(15,6) NOT NULL, [SHAPE] geography, [LEN] NUMERIC(15,6) NOT NULL";
						else
							columnString += "[FROMLONG] NUMERIC(15,6) NOT NULL, [FROMLAT] NUMERIC(15,6) NOT NULL, [TOLONG] NUMERIC(15,6) NOT NULL, [TOLAT] NUMERIC(15,6) NOT NULL, [LINEDATA] VARCHAR(MAX), [LEN] NUMERIC(15,6) NOT NULL";
						if (!StreetTableCreated)
						{
							if (con != null && con.State != ConnectionState.Open) con.Open();
							cmd = con.CreateCommand();
							cmd.CommandTimeout = 300;
							cmd.CommandText = "CREATE TABLE " + myInput.ImporterInput.RoadNetworkDatabaseName + "_Streets (" + columnString + ",CONSTRAINT [PK_" +
								  myInput.ImporterInput.RoadNetworkDatabaseName + "_Streets] PRIMARY KEY CLUSTERED" + Environment.NewLine + "([LINK_ID] ASC)) ON [PRIMARY]";
							cmd.CommandType = CommandType.Text;
							cmd.ExecuteNonQuery();

							cmd.CommandText = string.Empty;
							if (myInput.SQLVersion == SQLVersionEnum.SQLServer2005)
								cmd.CommandText = "CREATE NONCLUSTERED INDEX IX_" + myInput.ImporterInput.RoadNetworkDatabaseName + "_Streets ON dbo." + myInput.ImporterInput.RoadNetworkDatabaseName +
									  "_Streets" + Environment.NewLine + "(LINK_ID) INCLUDE (SPEED_CAT, FROMLONG, FROMLAT, TOLONG, TOLAT) " + Environment.NewLine +
									  "WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]";
							else if (myInput.SQLVersion == SQLVersionEnum.SQLServer2008)
								// http://msdn.microsoft.com/en-us/library/bb934196.aspx
								cmd.CommandText = "CREATE SPATIAL INDEX SIX_" + myInput.ImporterInput.RoadNetworkDatabaseName + "_Streets ON " + myInput.ImporterInput.RoadNetworkDatabaseName + "_Streets(shape)";
							cmd.CommandType = CommandType.Text;
							if (cmd.CommandText != string.Empty) cmd.ExecuteNonQuery();

							StreetTableCreated = true;
							if (myInput.MyTraceSource != null)
								myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, DateTime.Now.ToLongTimeString() + ": Streets Table Created");
						}
						if (myInput.MyTraceSource != null)
							myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, DateTime.Now.ToLongTimeString() + ": Streets running (" + StreetsFile + ")");

						if (MyWorker.CancellationPending) break;

						for (int count = 0; count < nEntities; count++)
						{
							try
							{
								if (MyWorker.CancellationPending) break;
								shpObj = new ShapeLib.SHPObject();
								pshpObj = IntPtr.Zero;
								pshpObj = ShapeLib.SHPReadObject(ptrSHP, count);
								shpObj = (ShapeLib.SHPObject)(Marshal.PtrToStructure(pshpObj, typeof(ShapeLib.SHPObject)));
								if ((Marshal.GetLastWin32Error() != 0) && (pshpObj == IntPtr.Zero)) Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error(), Marshal.GetExceptionPointers());
								length = shpObj.nVertices;

								for (int field = 0; field < fieldCount; field++)
								{
									type = ShapeLib.DBFGetFieldInfo(ptrDBF, field, strFieldName, ref fieldWidth, ref decimals);
									switch (type)
									{
										case ShapeLib.DBFFieldType.FTDouble: cmdInsert += ShapeLib.DBFReadDoubleAttribute(ptrDBF, count, field).ToString() + ",";
											break;
										case ShapeLib.DBFFieldType.FTInteger: cmdInsert += ShapeLib.DBFReadIntegerAttribute(ptrDBF, count, field).ToString() + ",";
											break;
										case ShapeLib.DBFFieldType.FTLogical: cmdInsert += ShapeLib.DBFReadLogicalAttribute(ptrDBF, count, field).ToString() + ",";
											break;
										case ShapeLib.DBFFieldType.FTString: cmdInsert += "'" + ShapeLib.DBFReadStringAttribute(ptrDBF, count, field).Replace("'", "''") + "',";
											break;
										case ShapeLib.DBFFieldType.FTInvalid:
										default:
											throw new Exception("Invalid field type in file: " + StreetsFile);
									}
								}
								Xarr = new double[length];
								Yarr = new double[length];

								Marshal.Copy(shpObj.padfX, Xarr, 0, length);
								Marshal.Copy(shpObj.padfY, Yarr, 0, length);

								Linedata = string.Empty;
								streetLen = 0;
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
									if (i < length - 1) Linedata += ", ";
									if (i > 0) streetLen += GreatCircleDistance.LinearDistance2(Yarr[i - 1], Xarr[i - 1], Yarr[i], Xarr[i], LinearUnitTypes.Miles);
								}

								if (myInput.SQLVersion == SQLVersionEnum.SQLServer2008)
								{
									// http://msdn.microsoft.com/en-us/library/bb933976.aspx
									// Linedata = "geography::STGeomFromText('LINESTRING(" + Linedata + ")', 4326)";                  
									shape = SqlGeography.Parse("LINESTRING(" + Linedata + ")");
									streetLen = shape.STLength().Value / 1609.344;
									shape = shape.Reduce(3.0);
								}
								else Linedata = "'" + Linedata + "'";
								if (con != null && con.State != ConnectionState.Open) con.Open();
								cmdInsert += "@shape, " + streetLen + " )";
								cmd = con.CreateCommand();
								cmd.CommandTimeout = 300;
								cmd.CommandText = cmdInsertQuery + cmdInsert;
								cmd.Parameters.AddWithValue("@shape", shape);
								cmd.Parameters["@shape"].UdtTypeName = "geography";
								cmd.CommandType = CommandType.Text;
								cmd.ExecuteNonQuery();
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
								if (mcount > 4) throw new Exception("Repeated error found in this shape file. Breaking the loop...");
							}
							finally
							{
								if (pshpObj != IntPtr.Zero)
								{
									ShapeLib.SHPDestroyObject(pshpObj);
									pshpObj = IntPtr.Zero;
								}
								completedPercent += 1 * currentFilePercent / nEntities;
								status = StreetsFile.Substring(myInput.ImporterInput.RootDirectory.Length, StreetsFile.Length - myInput.ImporterInput.RootDirectory.Length);
								status += Environment.NewLine + NavteqImporter.GetRemainingTime(start, completedPercent);
								MyWorker.ReportProgress(Convert.ToInt32(completedPercent), status);
								cmdInsert = string.Empty;
							}
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
						if (pshpObj != IntPtr.Zero)
						{
							ShapeLib.SHPDestroyObject(pshpObj);
							pshpObj = IntPtr.Zero;
						}
						cmdInsert = string.Empty;
						if (ptrDBF != IntPtr.Zero) { ShapeLib.DBFClose(ptrDBF); ptrDBF = IntPtr.Zero; }
						if (ptrSHP != IntPtr.Zero) { ShapeLib.SHPClose(ptrSHP); ptrSHP = IntPtr.Zero; }
						GC.Collect();
						GC.WaitForPendingFinalizers();
					}

					#endregion
				}
				foreach (string CDMSFile in CDMSFiles)
				{
					#region CDMS File

					currentFilePercent = (new FileInfo(CDMSFile)).Length * dbfTimeRatio * 100 / totalFilesBytes;
					try
					{
						decimals = 0;
						fieldWidth = 0;
						NoOfDBFRec = 0;
						msg = string.Empty;
						pmsg = string.Empty;
						mcount = 0;

						fType = ShapeLib.DBFFieldType.FTInvalid;

						ptrDBF = IntPtr.Zero;
						ptrDBF = ShapeLib.DBFOpen(CDMSFile, "r+b");
						if ((Marshal.GetLastWin32Error() != 0) && (ptrDBF == IntPtr.Zero))
							Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error(), Marshal.GetExceptionPointers());
						NoOfDBFRec = ShapeLib.DBFGetRecordCount(ptrDBF);
						if (Marshal.GetLastWin32Error() != 0) Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error(), Marshal.GetExceptionPointers());
						strFieldName = new StringBuilder(System.String.Empty);
						// string file = Path.GetFileName(CDMSFile).ToUpper();

						cmdInsertQuery = "INSERT INTO " + myInput.ImporterInput.RoadNetworkDatabaseName + "_CDMS ( ";
						columnString = string.Empty;
						fieldCount = ShapeLib.DBFGetFieldCount(ptrDBF);
						if (Marshal.GetLastWin32Error() != 0) Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error(), Marshal.GetExceptionPointers());

						for (int i = 0; i < fieldCount; i++)
						{
							strFieldName.Append(string.Empty);
							cmdInsertQuery += "[";
							columnString += "[";
							fType = ShapeLib.DBFGetFieldInfo(ptrDBF, i, strFieldName, ref fieldWidth, ref decimals);
							columnString += strFieldName + "] ";
							cmdInsertQuery += strFieldName + "], ";
							switch (fType)
							{
								case ShapeLib.DBFFieldType.FTDouble: columnString += "NUMERIC(15,6) NOT NULL, ";
									break;
								case ShapeLib.DBFFieldType.FTInteger: columnString += "INT NOT NULL, ";
									break;
								case ShapeLib.DBFFieldType.FTLogical: columnString += "BOOL, ";
									break;
								case ShapeLib.DBFFieldType.FTString: columnString += "VARCHAR(255), ";
									break;
								default:
									throw new Exception("Invalid field type in file: " + CDMSFile);
							}
						}
						columnString = columnString.Remove(columnString.Length - 2, 2);
						columnString += string.Empty;
						cmdInsertQuery = cmdInsertQuery.Remove(cmdInsertQuery.Length - 2, 2);
						cmdInsertQuery += ")";
						cmdInsertQuery += " VALUES (";

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

						if (MyWorker.CancellationPending) break;

						for (int count = 0; count < NoOfDBFRec; count++)
						{
							try
							{
								if (MyWorker.CancellationPending) break;
								cmdInsert = string.Empty;
								for (int field = 0; field < fieldCount; field++)
								{
									type = ShapeLib.DBFGetFieldInfo(ptrDBF, field, strFieldName, ref fieldWidth, ref decimals);
									switch (type)
									{
										case ShapeLib.DBFFieldType.FTDouble: cmdInsert += ShapeLib.DBFReadDoubleAttribute(ptrDBF, count, field).ToString() + ",";
											break;
										case ShapeLib.DBFFieldType.FTInteger: cmdInsert += ShapeLib.DBFReadIntegerAttribute(ptrDBF, count, field).ToString() + ",";
											break;
										case ShapeLib.DBFFieldType.FTLogical: cmdInsert += ShapeLib.DBFReadLogicalAttribute(ptrDBF, count, field).ToString() + ",";
											break;
										case ShapeLib.DBFFieldType.FTString: cmdInsert += "'" + ShapeLib.DBFReadStringAttribute(ptrDBF, count, field).Replace("'", "''") + "',";
											break;
										case ShapeLib.DBFFieldType.FTInvalid:
										default:
											throw new Exception("Invalid field type in file: " + CDMSFile);
									}
								}
								cmdInsert = cmdInsert.Remove(cmdInsert.Length - 1, 1);
								cmdInsert += ")";
								if (con != null && con.State != ConnectionState.Open) con.Open();
								cmd = con.CreateCommand();
								cmd.CommandTimeout = 300;
								cmd.CommandText = cmdInsertQuery + " " + cmdInsert;
								cmd.CommandType = CommandType.Text;
								cmd.ExecuteNonQuery();
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
								completedPercent += 1 * currentFilePercent / NoOfDBFRec;
								status = CDMSFile.Substring(myInput.ImporterInput.RootDirectory.Length, CDMSFile.Length - myInput.ImporterInput.RootDirectory.Length);
								status += Environment.NewLine + NavteqImporter.GetRemainingTime(start, completedPercent);
								MyWorker.ReportProgress(Convert.ToInt32(completedPercent), status);
							}
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
						if (ptrDBF != IntPtr.Zero) { ShapeLib.DBFClose(ptrDBF); ptrDBF = IntPtr.Zero; }
						GC.Collect();
						GC.WaitForPendingFinalizers();
					}

					#endregion
				}
				foreach (string RDMSFile in RDMSFiles)
				{
					#region RDMS File

					currentFilePercent = (new FileInfo(RDMSFile)).Length * dbfTimeRatio * 100 / totalFilesBytes;
					try
					{
						decimals = 0;
						fieldWidth = 0;
						msg = string.Empty;
						pmsg = string.Empty;
						mcount = 0;

						fType = ShapeLib.DBFFieldType.FTInvalid;

						ptrDBF = IntPtr.Zero;
						ptrDBF = ShapeLib.DBFOpen(RDMSFile, "r+b");
						if ((Marshal.GetLastWin32Error() != 0) && (ptrDBF == IntPtr.Zero))
							Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error(), Marshal.GetExceptionPointers());
						NoOfDBFRec = 0;
						NoOfDBFRec = ShapeLib.DBFGetRecordCount(ptrDBF);
						if (Marshal.GetLastWin32Error() != 0) Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error(), Marshal.GetExceptionPointers());
						strFieldName = new StringBuilder(System.String.Empty);
						// string file = Path.GetFileName(RDMSFile).ToUpper();

						cmdInsertQuery = "INSERT INTO " + myInput.ImporterInput.RoadNetworkDatabaseName + "_RDMS ( ";
						columnString = string.Empty;
						fieldCount = ShapeLib.DBFGetFieldCount(ptrDBF);

						for (int i = 0; i < fieldCount; i++)
						{
							strFieldName.Append(string.Empty);
							cmdInsertQuery += "[";
							columnString += "[";
							fType = ShapeLib.DBFFieldType.FTInvalid;
							fType = ShapeLib.DBFGetFieldInfo(ptrDBF, i, strFieldName, ref fieldWidth, ref decimals);
							if ((Marshal.GetLastWin32Error() != 0) && (fType == ShapeLib.DBFFieldType.FTInvalid))
								Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error(), Marshal.GetExceptionPointers());
							columnString += strFieldName + "] ";
							cmdInsertQuery += strFieldName + "], ";
							switch (fType)
							{
								case ShapeLib.DBFFieldType.FTDouble: columnString += "NUMERIC(15,6) NOT NULL, ";
									break;
								case ShapeLib.DBFFieldType.FTInteger: columnString += "INT NOT NULL, ";
									break;
								case ShapeLib.DBFFieldType.FTLogical: columnString += "BOOL, ";
									break;
								case ShapeLib.DBFFieldType.FTString: columnString += "VARCHAR(255), ";
									break;
							}
						}
						columnString = columnString.Remove(columnString.Length - 2, 2);
						columnString += string.Empty;
						cmdInsertQuery = cmdInsertQuery.Remove(cmdInsertQuery.Length - 2, 2);
						cmdInsertQuery += ")";
						cmdInsertQuery += " VALUES (";

						if (!RDMSTableCreated)
						{
							if (con != null && con.State != ConnectionState.Open) con.Open();
							cmd = con.CreateCommand();
							cmd.CommandTimeout = 300;
							cmd.CommandText = "CREATE TABLE " + myInput.ImporterInput.RoadNetworkDatabaseName + "_RDMS (" + columnString + ",CONSTRAINT [PK_" +
							  myInput.ImporterInput.RoadNetworkDatabaseName + "_RDMS] PRIMARY KEY CLUSTERED" + Environment.NewLine +
							  "([COND_ID] ASC,	[SEQ_NUMBER] ASC)) ON [PRIMARY]";
							cmd.CommandType = CommandType.Text;
							cmd.ExecuteNonQuery();
							RDMSTableCreated = true;
							if (myInput.MyTraceSource != null)
								myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, DateTime.Now.ToLongTimeString() + ": RDMS Table Created");
						}
						if (myInput.MyTraceSource != null)
							myInput.MyTraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, DateTime.Now.ToLongTimeString() + ": RDMS running (" + RDMSFile + ")");

						if (MyWorker.CancellationPending) break;

						for (int count = 0; count < NoOfDBFRec; count++)
						{
							try
							{
								if (MyWorker.CancellationPending) break;
								cmdInsert = string.Empty;
								for (int field = 0; field < fieldCount; field++)
								{
									type = ShapeLib.DBFFieldType.FTInvalid;
									type = ShapeLib.DBFGetFieldInfo(ptrDBF, field, strFieldName, ref fieldWidth, ref decimals);
									if ((Marshal.GetLastWin32Error() != 0) && (type == ShapeLib.DBFFieldType.FTInvalid))
										Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error(), Marshal.GetExceptionPointers());
									switch (type)
									{
										case ShapeLib.DBFFieldType.FTDouble: cmdInsert += ShapeLib.DBFReadDoubleAttribute(ptrDBF, count, field).ToString() + ",";
											break;
										case ShapeLib.DBFFieldType.FTInteger: cmdInsert += ShapeLib.DBFReadIntegerAttribute(ptrDBF, count, field).ToString() + ",";
											break;
										case ShapeLib.DBFFieldType.FTLogical: cmdInsert += ShapeLib.DBFReadLogicalAttribute(ptrDBF, count, field).ToString() + ",";
											break;
										case ShapeLib.DBFFieldType.FTString: cmdInsert += "'" + ShapeLib.DBFReadStringAttribute(ptrDBF, count, field).Replace("'", "''") + "',";
											break;
									}
								}
								cmdInsert = cmdInsert.Remove(cmdInsert.Length - 1, 1);
								cmdInsert += ")";
								if (con != null && con.State != ConnectionState.Open) con.Open();
								cmd = con.CreateCommand();
								cmd.CommandTimeout = 300;
								cmd.CommandText = cmdInsertQuery + " " + cmdInsert;
								cmd.CommandType = CommandType.Text;
								cmd.ExecuteNonQuery();
								ret.ProcessedInsertCount++;
							}
							catch (Exception ex)
							{
								msg = DateTime.Now.ToLongTimeString() + ": NavteqImporter SaveToSQL: An error occured during CDMS record process: " + ex.ToString();
								if (myInput.MyTraceSource != null)
									myInput.MyTraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
								ret.ErrorCount++;
							}
							finally
							{
								if ((count % 1000) == 0) con.Close();
								completedPercent += 1 * currentFilePercent / NoOfDBFRec;
								status = RDMSFile.Substring(myInput.ImporterInput.RootDirectory.Length, RDMSFile.Length - myInput.ImporterInput.RootDirectory.Length);
								status += Environment.NewLine + NavteqImporter.GetRemainingTime(start, completedPercent);
								MyWorker.ReportProgress(Convert.ToInt32(completedPercent), status);
							}
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
						if (ptrDBF != IntPtr.Zero) { ShapeLib.DBFClose(ptrDBF); ptrDBF = IntPtr.Zero; }
					}

					#endregion
				}
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
				if (ptrDBF != IntPtr.Zero) { ShapeLib.DBFClose(ptrDBF); ptrDBF = IntPtr.Zero; }
				if (ptrSHP != IntPtr.Zero) { ShapeLib.SHPClose(ptrSHP); ptrSHP = IntPtr.Zero; }
				// errCode = Marshal.GetLastWin32Error();
				// if (errCode != 0) Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error(), Marshal.GetExceptionPointers());
				con.Close();
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