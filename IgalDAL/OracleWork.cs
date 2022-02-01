using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace IgalDAL
{
    public class OracleWork : IDisposable
    {
        String sServerName, sUserName, sPassword;
        string sConnectionString;
        OracleConnection con = new OracleConnection();
        String sCurrentQrySql;
        //OracleDataAdapter daQry;
        private StringBuilder sbInfoMessage = new StringBuilder();
        const int DefaultReadBatchSize = 10;

        public bool ScriptHasDBMS { get; set; }

        public string InfoMessage { get { return sbInfoMessage.ToString(); } }

        //igal 27/12/21 - get the server messages 
        void con_InfoMessage(object sender, OracleInfoMessageEventArgs e)
        {            sbInfoMessage.AppendLine(e.Message);        }

        #region Technicals
        public OracleWork()
        {
            con.InfoMessage += con_InfoMessage;            
        }

        public void Dispose()
        {
            if (con.State == ConnectionState.Open)
            {
                try
                {
                    DisableDbmsOutput();
                    con.Close();
                    con.Dispose();
                }
                catch (Exception ex)
                { throw ex; }
            }
        }
        #endregion

        #region DBMS
        public void EnableDbmsOutput()
        {
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "DBMS_OUTPUT.ENABLE";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.ExecuteNonQuery();
            }
        }
        public void DisableDbmsOutput()
        {
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "DBMS_OUTPUT.DISABLE";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.ExecuteNonQuery();
            }
        }
        public List<string> ReadDbmsOutput(int readBatchSize = DefaultReadBatchSize)
        {
            if (readBatchSize <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(readBatchSize), "must be greater than zero");
            }
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "DBMS_OUTPUT.GET_LINES";
                cmd.CommandType = CommandType.StoredProcedure;
                var linesParam = cmd.Parameters.Add(new OracleParameter("lines", OracleDbType.Varchar2, int.MaxValue, ParameterDirection.Output));
                linesParam.CollectionType = OracleCollectionType.PLSQLAssociativeArray;
                linesParam.Size = readBatchSize;
                linesParam.ArrayBindSize = Enumerable.Repeat(32767, readBatchSize).ToArray();   // set bind size for each array element
                var numLinesParam = cmd.Parameters.Add(new OracleParameter("numlines", OracleDbType.Int32, ParameterDirection.InputOutput));
                var result = new List<string>();
                int numLinesRead;
                do
                {
                    numLinesParam.Value = readBatchSize;
                    cmd.ExecuteNonQuery();
                    numLinesRead = ((OracleDecimal)numLinesParam.Value).ToInt32();
                    var values = (OracleString[])linesParam.Value;
                    for (int i = 0; i < numLinesRead; i++)
                    {
                        result.Add(values[i].ToString());
                    }
                } while (numLinesRead == readBatchSize);
                return result;
            }
        }
        #endregion

        #region ConnectionProperties
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

        public OracleConnection Connect()
        {
            if (con.State != ConnectionState.Open)
            {                
                con.ConnectionString = ConnectionString;            
                try
                {   
                    con.Open();
                    EnableDbmsOutput();
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

        #endregion

        #region SQL Actions
        public DataSet OpenQry(String sSql , params OracleParameter[] params1)
        {
            DataSet dsQryFinal = new DataSet();
            
            int iQryCount = 0;
            string[] sSeparator = { "--newquery" };
            string[] qrys = sSql.Split(sSeparator, System.StringSplitOptions.RemoveEmptyEntries);

            foreach (string sql in qrys)
            {                  
                string sqlTemp = sql.Trim();

                if (sqlTemp.Substring(sqlTemp.Length-1) == ";")
                    sqlTemp = sqlTemp.Substring(0, sqlTemp.Length - 1);

                iQryCount++;
                //igal 23/7/19 console                    
                var watch = System.Diagnostics.Stopwatch.StartNew();                                        
                Console.WriteLine($"StartQuery {((qrys.Length > 1) ? iQryCount.ToString() : "")}-{DateTime.Now.ToString("hh:mm:ss")}" );
                DataSet ds = new DataSet();
                ds = OpenSingleQry(sqlTemp, params1);
                watch.Stop(); 
                Console.WriteLine($"EndQuery {DateTime.Now.ToString("hh:mm:ss")}. Query execution time {watch.Elapsed.Duration()}");

                int iTable = 0;
                //igal 23/7/19                    
                foreach (DataTable dt in ds.Tables)
                {                    
                    if (dt.Columns.Count > 0)
                    {
                        dt.TableName = "Table" + (iQryCount) + "_" + ++iTable;
                        dsQryFinal.Tables.Add(dt.Copy());
                    }
                }
            }

            sCurrentQrySql = sSql;
            //if(daQry != null)
            //    daQry.Dispose();
            return dsQryFinal;
        }

        public DataSet OpenSingleQry(String sSql, params OracleParameter[] params1)
        {
            DataSet ds = new DataSet();
            string sFinalSql = "";            

            //8/12/20
            int iIndex = sSql.IndexOf("call ");
            if (iIndex > -1)
            {                
                //extract procedure name
                sFinalSql = sSql.Substring(iIndex + "call ".Length, sSql.Length - iIndex - "call ".Length).Trim();
                int iIndexSpace = sFinalSql.IndexOf(" ");
                int iIndexLeftBracket = sFinalSql.IndexOf("(");
                int iIndexFinal = (iIndexLeftBracket > -1 && iIndexLeftBracket < iIndexSpace) ? iIndexLeftBracket : iIndexSpace;
                if (iIndexFinal > -1)
                {                                                            
                    sFinalSql = sFinalSql.Substring(0, iIndexFinal);
                    string sParams = sSql.Remove(0, iIndex + "call ".Length + iIndexFinal).Trim().Replace("(","").Replace(")","").Replace(":","").Replace("&","");
                    string[] sParamsFromScript = sParams.Split(',');
                    DataTable dtArgs = GetProcedureArgs(sFinalSql);
                    OracleParameter[] finalParams = new OracleParameter[dtArgs.Rows.Count];
                    for (int i=0;i < dtArgs.Rows.Count; i++)
                    {
                        DataRow row = dtArgs.Rows[i];
                        finalParams[i] = new OracleParameter();

                        switch (row["in_out"])
                        {
                            case "IN":
                                finalParams[i].Direction = ParameterDirection.Input;
                                var val = params1.Where(item => item.ParameterName.ToUpper() == row["argument_name"].ToString()).FirstOrDefault();
                                if (val != null)
                                    finalParams[i].Value = val.Value;
                                else
                                    finalParams[i].Value = sParamsFromScript[i];
                                break;
                            case "OUT":
                                finalParams[i].Direction = ParameterDirection.Output;
                                break;
                            case "IN/OUT":
                                finalParams[i].Direction = ParameterDirection.InputOutput;
                                finalParams[i].Value = params1.Where(item => item.ParameterName == row["argument_name"].ToString()).FirstOrDefault();
                                break;
                            default:
                                throw new Exception($"unknow parameter direction {row["in_out"]} in paramer {row["argument_name"]} for procedure {sFinalSql}");
                        }
                        finalParams[i].ParameterName = row["argument_name"].ToString();

                        try
                        {
                            switch (row["data_type"])
                            {
                                case "NUMBER":
                                    finalParams[i].OracleDbType = OracleDbType.Decimal;
                                    break;
                                default:
                                    finalParams[i].OracleDbType = (OracleDbType)Enum.Parse(typeof(OracleDbType), row["data_type"].ToString().Replace(" ", ""), true);
                                    break;
                            }
                        }
                        catch (Exception ex)
                        {
                            throw new Exception($"Error converting parameter type {row["data_type"]} of paramer {row["argument_name"]} for procedure {sFinalSql}");
                        }                        
                    }
                    ds = ExecuteProcedureWithResult(sFinalSql, finalParams);
                }
            }
            else
            {
                List<OracleParameter> oraParams = new List<OracleParameter>();
                if (params1 != null)
                {
                    
                    oraParams.AddRange(params1);
                    foreach (OracleParameter param in params1)
                    {
                        //igal 24/7/19
                        if (param.ParameterName.Substring(0, 1) == "&" && param.ParameterName.Substring(param.ParameterName.Length - 1, 1) == "&" && sSql.Contains(param.ParameterName))
                        {
                            //replace &type-parameter with the same parameter in sql and then remove it from params in query
                            sSql = sSql.Replace(param.ParameterName, param.Value.ToString());
                            oraParams.Remove(param);
                        }
                        else
                        {
                            if (!sSql.ToUpper().Contains(":" + param.ParameterName.ToUpper()) & !sSql.ToUpper().Contains("&" + param.ParameterName.ToUpper()))
                                oraParams.Remove(param);
                            //igal 12/9/21
                            if (sSql.ToLower().Contains("&" + param.ParameterName.ToLower()))
                                sSql = sSql.ToLower().Replace("&" + param.ParameterName.ToLower(), ":" + param.ParameterName.ToLower());
                        }
                    }
                }
                sFinalSql = sSql;
                ds = RunScript(sFinalSql, oraParams.ToArray());
            }                                            

            sCurrentQrySql = sSql;

            return ds;
        }

        //igal 17/12/20
        public DataTable GetProcedureArgs(string  ProcedureName)
        {
            string sProcedureName = "", sPackageName = "", sSchemaName = "";
            string [] sSeparate;
            string sSql = "";

            //check if procedure name has a dot (.) - separate it into packagename and procedure name            
            sSeparate = ProcedureName.Split('.');
            //last member is always a procedure name
            if (sSeparate.Length > 0)
                sProcedureName = sSeparate[sSeparate.Length - 1];
            else
                sProcedureName = ProcedureName;

            sSql = $"select * from SYS.ALL_ARGUMENTS where upper(object_name) = upper('{sProcedureName}')";

            switch (sSeparate.Length)
            {
                case 2:
                    //try first package name. if not - make it schema
                    sSql += $" and (upper(package_name) = upper ('{sSeparate[0]}') or upper(owner) = upper('{sSeparate[0]}'))";
                    break;
                case 3:
                    //in this case - 1st member is schema_name, second - package
                    sSchemaName = sSeparate[0];
                    sPackageName = sSeparate[1];
                    sSql += $" and upper(package_name) = upper ('{sPackageName}')";
                    break;
                default:
                    break;
            }

            sSql += "ORDER BY Sequence";

            DataSet ds = RunScript(sSql, null);
            if (ds.Tables.Count > 0)
                return ds.Tables[0];
            return null;
        }

        public int UpdateDB_LastQry(DataSet ds)
        {
            int iAffected = 0;
            using (OracleDataAdapter daQry = new OracleDataAdapter(sCurrentQrySql, con))
            { 
                daQry.UpdateCommand = new OracleCommandBuilder(daQry).GetUpdateCommand();
                iAffected = daQry.Update(ds);
                return iAffected;
            }
        }

        public DataSet RunScript(string sql, params OracleParameter[] @params)
        {
            DataSet ds = new DataSet();
            DataTable dt = new DataTable();
            OracleDataReader reader;
            OracleCommand cmdQry = new OracleCommand();

            cmdQry.CommandText = sql;
            if (@params != null && @params.Length > 0)
            { cmdQry.Parameters.AddRange(@params); }

            cmdQry.CommandType = CommandType.Text;
            cmdQry.Connection = Connect();
            cmdQry.BindByName = true; //igal 20/1/20 - without that there's an importance of passing parameters by order

            try
            {
                reader = cmdQry.ExecuteReader();
                while (!reader.IsClosed)
                {
                    dt.Load(reader);
                    ds.Tables.Add(dt);
                    if (!reader.IsClosed) reader.NextResult();
                }
                return ds;
            }
            catch (Exception ex)
            {
                string ParamsProps = GetParamsPropsCSV(@params);
                throw new Exception($"Error executing procedure {sql}\nPassed params:\n{ParamsProps}", ex);
            }
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
            { 
                string ParamsProps = GetParamsPropsCSV(@params);
                throw new Exception($"Error executing procedure {sProcedureName}\nPassed params:\n{ParamsProps}", ex);
            }

        }

        public bool ExecuteCommand(String SqlCommand, params OracleParameter[] @params)
        {
            OracleCommand cmdQry = new  OracleCommand();

            cmdQry.CommandText = SqlCommand;
            if (@params != null && @params.Length > 0 )
            {   cmdQry.Parameters.AddRange(@params);    }
        
            cmdQry.CommandType = CommandType.Text;
            cmdQry.Connection = Connect();

            try
            {
                cmdQry.ExecuteNonQuery();
                return true;
            }
            catch (Exception ex )
            {
                string ParamsProps = GetParamsPropsCSV(@params);
                throw new Exception($"Error running script command\nPassed params:\n{ParamsProps} \n{SqlCommand}", ex);
            }                    
        }

        public DataSet ExecuteCommandWithResult(String SqlCommand, params OracleParameter[] @params)
        {
            return RunWithResult(SqlCommand, CommandType.Text, @params);
        }

        public DataSet ExecuteProcedureWithResult(String sProcedureName, params OracleParameter[] @params ) 
        {
            return RunWithResult(sProcedureName, CommandType.StoredProcedure, @params);
        }

        private DataSet RunWithResult(string SqlCommand, CommandType comType, params OracleParameter[] @params)
        {
            OracleCommand cmdQry = new OracleCommand(SqlCommand, Connect());
            DataSet ds = new DataSet();

            if (@params.Length > 0)
            { cmdQry.Parameters.AddRange(@params); }

            cmdQry.CommandType = comType;
            cmdQry.BindByName = true; //igal 20/1/20 - without that there's an importance of passing parameters by order

            try
            {
                using (OracleDataAdapter da = new OracleDataAdapter(cmdQry))
                {
                    da.Fill(ds);
                    return ds;
                }                 
            }
            catch (Exception ex)
            {
                string ParamsProps = GetParamsPropsCSV(@params);
                throw new Exception($"Error running script command\nPassed params:\n{ParamsProps} \n{SqlCommand}", ex);
            }
        }

        public DataSet RunCommand_AutoParse(String FullScript, params OracleParameter[] @params)
        {
            DataSet ds = new DataSet();
            string sScriptForCheck = FullScript;
            try
            {
                ScriptNormalizer normalizer = new ScriptNormalizer(FullScript);
                sScriptForCheck = normalizer.GetNormalizedScript(); //used to remove all remarks from scripts for cases begin or dbms_output.put are remarked
            }
            catch (Exception)
            {               
                //there're some cases normalizer fails - in this case leave the check on fullscript
            }            
            int iBeginPosition = sScriptForCheck.ToLower().IndexOf($"begin{Environment.NewLine}");
            if (iBeginPosition > -1 && FullScript.ToLower().IndexOf("end;") > -1)
            {
                foreach(OracleParameter param in @params)
                {
                    if (FullScript.ToLower().Contains("&" + param.ParameterName.ToLower()))
                        FullScript = FullScript.ToLower().Replace("&" + param.ParameterName.ToLower(), ":" + param.ParameterName.ToLower());
                }                 
                ds = ExecuteCommandWithResult(FullScript, @params);
            }
            else
            {
               ds = OpenQry(FullScript, @params);
            }

            ScriptHasDBMS = (sScriptForCheck.ToLower().IndexOf("dbms_output.put") > -1);

            return ds;
        }
        #endregion

        private string GetParamsPropsCSV(params OracleParameter[] @params)
        {
            string[] paramsKeyValue;
            paramsKeyValue = @params.Select((item) =>
            {
                string value = (item.Value == null) ? "" : "= " + item.Value.ToString();
                return $"{item.ParameterName} {value} : {item.OracleDbType.ToString()} {item.Direction}";
            }).ToArray();
            return string.Join<string>("\n", paramsKeyValue);
        }
    }
}
