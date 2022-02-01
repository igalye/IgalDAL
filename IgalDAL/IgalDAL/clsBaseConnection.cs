using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
//using Oracle.DataAccess.Client;

namespace IgalDAL
{
    public abstract class clsBaseConnection
    {
        protected SqlConnection m_SqlCn;
        //protected OracleConnection m_OraCn;
        protected bool m_mustDisposeCN;
        protected static SqlConnectionStringBuilder m_Cnstr;                

        public clsBaseConnection()
        {
            if (m_Cnstr == null)
            {
                if (ConnectionString != "")
                    m_Cnstr = new SqlConnectionStringBuilder(ConnectionString);
                else
                    m_Cnstr = new SqlConnectionStringBuilder();
            }
            m_mustDisposeCN = true;            
            SqlDAC.SetCommandTimeOut(200);
            LoadConnectionString();
        }

        public clsBaseConnection(string ConnectionString)
        {
            m_mustDisposeCN = true;
            m_Cnstr = new SqlConnectionStringBuilder(ConnectionString);            

            SqlDAC.SetCommandTimeOut(SqlDAC.TimeOut);
            
        }

        protected SqlConnectionStringBuilder ConnectionBuilder
        { get { return m_Cnstr; } }

        internal protected string ConnectionString
        { get { return m_Cnstr.ConnectionString; } set { m_Cnstr.ConnectionString = value; } }

        private void LoadConnectionString()
        {     
            if(m_Cnstr == null && m_Cnstr.ConnectionString == "")       
                throw new Exception ("Connection not initialized");
        }

       protected SqlDataReader ExecuteReader(CommandType commandType , 
                                      String commandText, 
                                      params SqlParameter[]  commandParameters) 
        {           
            return SqlDAC.ExecuteReader(m_Cnstr.ConnectionString, commandType, commandText, commandParameters);            
        }

       protected DataSet ExecuteDataset(CommandType commandType, String commandText, params SqlParameter[] commandParameters) 
       {
           return SqlDAC.ExecuteDataset(m_Cnstr.ConnectionString, commandType, commandText, commandParameters);
       }

       protected DataSet ExecuteDataset(SqlConnection con, CommandType commandType, String commandText, params SqlParameter[] commandParameters)
       {
           return SqlDAC.ExecuteDataset(con, commandType, commandText, commandParameters);
       }

       protected object ExecuteScalar(CommandType commandType, String commandText,
                                                        params SqlParameter[] commandParameters)
       {
           return SqlDAC.ExecuteScalar(m_Cnstr.ConnectionString, commandType, commandText, commandParameters);
       }

        protected int ExecuteNonQuery(CommandType commandType, String commandText, params SqlParameter[] commandParameters)
       {
           return SqlDAC.ExecuteNonQuery(m_Cnstr.ConnectionString, commandType, commandText, commandParameters);
       }

       protected int ExecuteNonQuery(SqlConnection con, CommandType commandType, String commandText, params SqlParameter[] commandParameters) 
        {
            return SqlDAC.ExecuteNonQuery(con, commandType, commandText, commandParameters);
        }

    }
}
