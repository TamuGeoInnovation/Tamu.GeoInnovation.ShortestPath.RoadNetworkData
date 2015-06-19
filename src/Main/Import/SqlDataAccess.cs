/*******************************************************************
* Filename: SqldataAccess.cs			                                                                           *
*																							                                                                         *
* Details:	This file contains the code that is required to interface with the SQL Server    *
* database in order to be able to write into the database the data that is read from the  *
* Shape files                                                                                                      *
*                                                                                                                     *
*																							                                                                         *
* Date Created: 03/01/2008					                                                                         *
*																							                                                                         *
*																							                                                                         *
*																							                                                                         *
*																							                                                                         *
********************************************************************/

using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace USC.GISResearchLab.ShortestPath.RoadNetworkData
{
  //class handles SQL data connections 
  public class SqlDataAccess : DatabaseAccess
  {
    public override DbConnection openConnection(string connectionString)
    {
      connection = new SqlConnection(connectionString);
      connection.Open();
      return connection;
    }

    public override void closeConnection()
    {
      if (connection != null && connection.State == ConnectionState.Open)
      {
        connection.Close();
      }
    }

    public override DataSet getData(string query)
    {
      //base.getData();
      return null;
    }

    public override void setData()
    {
      //base.setData();
    }

    public override void updateData(DataSet ds, string tablename, string cmdText)
    {
      DataSet dsTemp = new DataSet();
      if (connection != null && connection.State == ConnectionState.Open)
      {

        SqlCommand insCmd = new SqlCommand(cmdText, (SqlConnection)connection);

        for (int i = 0; i < ds.Tables[0].Columns.Count; i++)
        {
          insCmd.Parameters.Add("@" + ds.Tables[0].Columns[i].ToString(), SqlDbType.VarChar, 100, ds.Tables[0].Columns[i].ToString());
        }
        SqlDataAdapter adapter = new SqlDataAdapter();
        adapter.InsertCommand = insCmd;
        dsTemp.Merge(ds, true);
        adapter.Update(dsTemp, tablename);
      }
    }

    public void SqlExecuteNonQuery(string query)
    {
      SqlCommand insCmd = new SqlCommand(query, (SqlConnection)connection);
      insCmd.CommandText = query;
      insCmd.CommandType = CommandType.Text;
      int result = insCmd.ExecuteNonQuery();
    }
  }
}