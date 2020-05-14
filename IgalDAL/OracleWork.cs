using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Oracle.DataAccess.Client;

namespace IgalDAL
{
    public class OracleWork : IDisposable
    {
        String sServerName, sUserName, sPassword;
        string sConnectionString;
        OracleConnection con = new OracleConnection();
        String sCurrentQrySql;
        OracleDataAdapter daQry;

        public OracleWork()
        {

        }

        #region "ConnectionProperties"
        public string ServerName 
        {
            get
            { return sServerName; }
            set
            { sServerName = value; }
        }

        public string Password 
        {
            get
            { return sPassword; } 
            set
            { sPassword = value; }
        }

        public string UserName
        {
            get
            { return sUserName;}        
            set
            { sUserName = value;}
        }
        #endregion

        public OracleConnection Connect()
        {
            if (con.State != ConnectionState.Open)
            {
                con.ConnectionString = ConnectionString;            
                try
                {   
                    con.Open(); 
                }
                catch (Exception ex )
                    {   throw ex;   }
            }
            return con;
        }

        public string ConnectionString 
        {
            get 
            {
                try
                {
                    if (sConnectionString!="")
                    {
                        return sConnectionString;
                    }
                    else
                    { 
                    OracleConnectionStringBuilder sCon = new OracleConnectionStringBuilder();
                    sCon.DataSource = sServerName;
                    sCon.UserID = sUserName;
                    sCon.Password = sPassword;
                    return sCon.ConnectionString;
                    }
                }
                catch (Exception ex)
                {                    
                    throw ex;
                }
            }
            set {sConnectionString = value;}
        }

        public void Dispose()
        {
            if (con.State == ConnectionState.Open)
            {
            try
            {
                con.Close();
                con.Dispose();
            }
            catch (Exception ex)
            { throw ex; }
            }
        }

        public DataSet OpenQry(String sSql , params OracleParameter[] params1)
        {
            DataSet dsQryFinal = new DataSet();
            DataTable dt;
            
            int iQryCount = 0;
            string[] sSeparator = { "--newquery" };
            string[] qrys = sSql.Split(sSeparator, System.StringSplitOptions.RemoveEmptyEntries);

            foreach (string sql in qrys)
            {
                //try
                //{                    
                    string sqlTemp = sql.Trim();

                    if (sqlTemp.Substring(sqlTemp.Length-1) == ";")
                        sqlTemp = sqlTemp.Substring(0, sqlTemp.Length - 1);

                    iQryCount++;
                    //igal 23/7/19 console                    
                    var watch = System.Diagnostics.Stopwatch.StartNew();                                        
                    Console.WriteLine(string.Format("StartQuery {0}-{1}" ,((qrys.Length>1)? iQryCount.ToString():""),DateTime.Now.ToString("hh:mm:ss")));
                    dt = OpenSingleQry(sqlTemp, params1);
                    watch.Stop(); 
                    Console.WriteLine(string.Format("EndQuery {0}. Query execution time {1}", DateTime.Now.ToString("hh:mm:ss"), watch.Elapsed.Duration()));
                    //igal 23/7/19
                    if (dt.Columns.Count>0)
                    {
                        dt.TableName = "Table" + (iQryCount);
                        dsQryFinal.Tables.Add(dt.Copy());
                    }
                //}
                //catch (Exception ex)
                //{
                //    throw ex;
                //}
            }

            sCurrentQrySql = sSql;
            daQry.Dispose();
            return dsQryFinal;
        }

        public DataTable OpenSingleQry(String sSql, params OracleParameter[] params1)
        {            
            DataTable dt = new DataTable();
            OracleCommand cmdQry = new OracleCommand();
            //OracleParameter[] paramTmp = null;
            //if (params1 != null)
            //{
            //    paramTmp = new OracleParameter[params1.Length];
            //    params1.CopyTo(paramTmp, 0);
            //}

            daQry = new OracleDataAdapter();

            cmdQry.CommandType = CommandType.Text;
            cmdQry.Connection = Connect();
            daQry.SelectCommand = cmdQry;            

            if (params1 != null && params1.Length > 0)
            { cmdQry.Parameters.AddRange(params1); }

            if (params1 != null)
            {
                foreach (OracleParameter param in params1)
                {
                    //igal 24/7/19
                    if (param.ParameterName.Substring(0, 1) == "&" && param.ParameterName.Substring(param.ParameterName.Length - 1, 1) == "&" && sSql.Contains(param.ParameterName))
                    {
                        //replace &type-parameter with the same parameter in sql and then remove it from params in query
                        sSql = sSql.Replace(param.ParameterName, param.Value.ToString());
                        cmdQry.Parameters.Remove(param);
                    }
                    else
                    {
                        if (!sSql.ToUpper().Contains(":" + param.ParameterName.ToUpper()) & !sSql.ToUpper().Contains("&" + param.ParameterName.ToUpper()))
                            cmdQry.Parameters.Remove(param);
                    }
                }
            }

            cmdQry.CommandText = sSql;

            daQry.Fill(dt);

            sCurrentQrySql = sSql;

            return dt;
        }

            public int UpdateDB_LastQry(DataSet ds)
        {        
            int iAffected=0;
            try
            {
                OracleDataAdapter daQry = new OracleDataAdapter(sCurrentQrySql, con);
                daQry.UpdateCommand = new OracleCommandBuilder(daQry).GetUpdateCommand();
                iAffected = daQry.Update(ds);
                return iAffected;
            }
            catch (Exception ex )
            {   throw ex;   }
            finally
            {   daQry.Dispose();    }
        }

        public bool ExecuteProcedure(String sProcedureName , params OracleParameter[] @params) 
        {
            OracleCommand cmdQry = new  OracleCommand();

            cmdQry.CommandText = sProcedureName;
            if (@params.Length > 0 )
            {
                cmdQry.Parameters.AddRange(@params);
            }
            cmdQry.CommandType = CommandType.StoredProcedure;
            cmdQry.Connection = Connect();

            try
            {
                cmdQry.ExecuteNonQuery();
                return true;
            }
            catch (Exception ex )
                {   throw ex;   }

        }

        public bool ExecuteCommand(String SqlCommand, params OracleParameter[] @params)
        {
            OracleCommand cmdQry = new  OracleCommand();

            cmdQry.CommandText = SqlCommand;
            if (@params.Length > 0 )
            {cmdQry.Parameters.AddRange(@params);}
        
            cmdQry.CommandType = CommandType.Text;
            cmdQry.Connection = Connect();

            try
            {
                cmdQry.ExecuteNonQuery();
                return true;
            }
            catch (Exception ex )
            {
                throw ex;
            }                    

        }

        public DataSet ExecuteProcedureWithResult(String sProcedureName, params OracleParameter[] @params ) 
        {
            OracleCommand cmdQry = new OracleCommand();
            DataSet ds = new DataSet();
            OracleDataAdapter da =new OracleDataAdapter();

            cmdQry.CommandText = sProcedureName;
            if (@params.Length > 0)
            {   cmdQry.Parameters.AddRange(@params); }        

            cmdQry.CommandType = CommandType.StoredProcedure;
            cmdQry.Connection = Connect();
            da.SelectCommand = cmdQry;

            try
            {
                da.Fill(ds);
                return ds;
            }
            catch (Exception ex )
            {throw ex;}        

        }

    }
}
