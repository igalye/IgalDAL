using System;
using System.Data;
using System.Data.SqlClient;
using System.Text;
namespace IgalDAL
{
    public static class PublicModule
    {
        public static string QuoteString(string s)
        {
            if (s == null || s.Trim()=="")
                return "";

            if (!s.StartsWith("'"))
                s = "'" + s;
            if (!s.EndsWith("'"))
                s += "'";
            return s;
        }

        public static Type ParseSqlTypeToSystemType(string DataType)
        {
            switch (DataType)
            {
                case "int":
                    DataType = "System.Int32";
                    break;
                case "date":
                    DataType = "System.DateTime";
                    break;
                case "string":
                    DataType = "System.String";
                    break;
                case "smallint":
                    DataType = "System.Int16";
                    break;
                default:
                    if (DataType.StartsWith("varchar"))
                        DataType = "System.String";
                    else
                        //tmp
                        DataType = "";
                    break;
            }
            try
            {
                if (DataType != "")
                    return System.Type.GetType(DataType);
                else
                    return null;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static string ParseSystemTypeToSqlType(Type DataType)
        {
            if (DataType == null)
                return "";

            switch (DataType.Name.ToLower())
            {
                case "int32":
                case "integer":
                    return "int";
                case "int16":
                    return "smallint";
                case "int64":
                    return "bigint";
                case "datetime":
                    return "date";
                case "single":
                case "double":
                    return "float";
                case "decimal":
                    //TODO - get the field length
                    return "decimal(14,3)";
                case "string":
                    //TODO - get the field length
                    int iFieldLen = 50;
                    return "varchar(" + iFieldLen + ")";
                default:
                    return "";
            }
        }

        public static void GetTableSpaceUsed(out decimal Size, out decimal UnUsed, string Connection, string tableName="")
        {
            string sSize="", sUnUsed="", sSizeColName = "database_size", sUnUsedColName="unallocated space";
            SqlParameter[] param = null;
            if (tableName.Trim() != "")
            {
                param = new SqlParameter[1];
                param[0] = new SqlParameter("@objname", tableName);
                sSizeColName = "data";
                sUnUsedColName = "unused"; 
            }

            try
            {
                DataSet ds = SqlDAC.ExecuteDataset(Connection, CommandType.StoredProcedure, "sp_spaceused", param);
                sSize = ds.Tables[0].Rows[0][sSizeColName].ToString();
                sUnUsed = ds.Tables[0].Rows[0][sUnUsedColName].ToString();

                Size = decimal.Parse(sSize.Substring(0,sSize.LastIndexOf(' ')));
                UnUsed = decimal.Parse(sUnUsed.Substring(0, sUnUsed.LastIndexOf(' ')));                
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static string CreateTempTableFromDataTable(ref SqlConnection con, System.Data.DataTable dt, bool bGlobalTable = true, string sTableName = "")
        {
            string tbl = "";

            if (sTableName.Trim().Length == 0)
            {
                Random rnd = new Random();
                tbl = ((bGlobalTable)?"##":"#") + "tbl_" + rnd.Next(10000).ToString();
            }
            else
	        {
                tbl = sTableName;
	        }

            StringBuilder sbTempTable = new StringBuilder("CREATE TABLE " + tbl + "(");
            string sColDef = "";
            foreach (DataColumn col in dt.Columns)
            {
                switch (col.DataType.ToString())
                {
                    case "System.Int64":
                        sColDef = "[" + col.ColumnName + "] bigint ";
                        sColDef += (col.AutoIncrement) ? " Identity (" + col.AutoIncrementSeed.ToString() + "," + col.AutoIncrementStep.ToString() + ")," : ",";
                        sbTempTable.AppendLine(sColDef);
                        break;
                    case "System.Int32":
                        sColDef = "[" + col.ColumnName + "] int ";
                        sColDef += (col.AutoIncrement) ? " Identity (" + col.AutoIncrementSeed.ToString() + "," + col.AutoIncrementStep.ToString() + ")," : ",";
                        sbTempTable.AppendLine(sColDef);
                        break;
                    case "System.DateTime":
                        sbTempTable.AppendLine("[" + col.ColumnName + "] datetime2, ");
                        break;
                    case "System.String":
                        sColDef = "[" + col.ColumnName + "] varchar( ";
                        sColDef += (col.MaxLength == -1) ? "max" : col.MaxLength.ToString();
                        sColDef += "), ";
                        sbTempTable.AppendLine(sColDef);
                        break;
                    case "System.Single":
                        sbTempTable.AppendLine("[" + col.ColumnName + "] float , ");
                        break;
                    case "System.Double":
                        sbTempTable.AppendLine("[" + col.ColumnName + "] float , ");
                        break;
                    case "System.Int16":
                        sbTempTable.AppendLine("[" + col.ColumnName + "] smallint , ");
                        break;
                    case "System.Boolean":
                        sbTempTable.AppendLine("[" + col.ColumnName + "] bit , ");
                        break;
                    case "System.Decimal":
                        sbTempTable.AppendLine("[" + col.ColumnName + "] decimal(19,4) , ");
                        break;
                    case "System.Byte":
                        sbTempTable.AppendLine("[" + col.ColumnName + "] tinyint, ");
                        break;
                    default:
                        break;
                }
            }

            string pks = "";
            if (dt.PrimaryKey.Length > 0)
            {
                pks = "CONSTRAINT PK_" + tbl + " PRIMARY KEY (";
                for (int i = 0; i < dt.PrimaryKey.Length; i++)
                {
                    pks += dt.PrimaryKey[i].ColumnName + ",";
                }
                pks = pks.Substring(0, pks.Length - 1) + ")";

            }
            if (pks != "")
                sbTempTable.AppendLine(pks);
            else
                sbTempTable.Remove(sbTempTable.Length - 1, 1);
            sbTempTable.Append(")");
            try
            {
                StringBuilder sDropTempTables = new StringBuilder();
                sDropTempTables.Append("IF OBJECT_ID('tempdb..");
                sDropTempTables.Append(tbl);
                sDropTempTables.AppendLine ("') IS NOT NULL");
                sDropTempTables.Append(" DROP TABLE ");
                sDropTempTables.Append(tbl);
                SqlDAC.ExecuteNonQuery(con, CommandType.Text, sDropTempTables.ToString(), null);

                int iTemp = SqlDAC.ExecuteNonQuery(con, CommandType.Text, sbTempTable.ToString(), null);
            }
            catch (Exception ex)
            {

                throw ex;
            }


            return tbl;
        }

        public static string Help()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("IgalDAL help:");
            sb.AppendLine("If making a request to an oracle DB with multiple queries:");
            sb.AppendLine("1. Insert a auxiliary line  --newquery");
            sb.AppendLine("2. There's no need to put a semicolon ';', but if it is - it should the the last character in a query");
            return sb.ToString();
        }
    }
}