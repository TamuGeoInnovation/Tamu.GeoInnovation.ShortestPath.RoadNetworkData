/*********************************************************************************************
* Filename: DataAccessBase.cs																 *
*																							 *
* Details:	This file contains the base class code for any database functions.               *
* This code can be resued if any other provider needs to be added to this project            *
* files in order to be able to read from them                                                *
*                                                                                            *
*                                                                                            *
*																							 *
* Date Created: 03/01/2008																	 *
*																							 *
*																							 *
*																							 *
*																							 *
*																							 *
*																							 *
*																							 *
*																							 *
*																							 *
**********************************************************************************************/
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;
using System.Data;

namespace USC.GISResearchLab.ShortestPath.RoadNetworkData
{
    //Abstract base class
    public abstract class DatabaseAccess
    {
        protected DbConnection connection = null;
        protected DbCommand command = null;


        public virtual DbConnection openConnection(string connectionString) { return connection; }
        public virtual void closeConnection() { }
        public virtual DataSet getData(string query) { return null; }
        public virtual void setData() { }
        public virtual void executeQuery(string query)
        {
            if (connection != null && connection.State == ConnectionState.Open)
            {
                command.CommandText = query;
                command.ExecuteNonQuery();
            }
        }

        public virtual void updateData(DataSet ds, string tablename, string cmdText)
        {
                  
        }

        public virtual void BeginTransaction()
        {
            if (connection != null && connection.State == ConnectionState.Open)
            {
                command.Transaction = connection.BeginTransaction();
            }  
        }
        public void CreateCommand()
        {
            command = connection.CreateCommand();
        }

        public void AlterCommand(string text)
        {
            command.CommandText = text;
        }

        public void CommitTransaction()
        {
            if (connection != null && connection.State == ConnectionState.Open && command != null && command.Transaction != null)
            {
                command.Transaction.Commit();
            }
        }
        public void RollbackTransaction()
        {
            if (connection != null && connection.State == ConnectionState.Open && command != null && command.Transaction != null)
            {
                command.Transaction.Rollback();
            }
        }
    }
}
