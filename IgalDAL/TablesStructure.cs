using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Collections;

namespace IgalDAL
{
    public class TablesStructure:clsBaseConnection
    {
        public enum JoinType
        {
            InnerJoin,
            LeftJoin
        }
        internal struct TableJoinType:IEnumerable
        {
            internal JoinType typeJoin;
            internal string sTableRelation;            

            public IEnumerator GetEnumerator()
            {
                throw new NotImplementedException();
            }
            internal void Add(JoinType _joinType) { typeJoin = _joinType; }
            internal void Add(string _TableRelation) { sTableRelation = _TableRelation; }

            public override string ToString()
            {
                switch (typeJoin)
                {
                    case JoinType.InnerJoin:
                        return "INNER JOIN ";
                    case JoinType.LeftJoin:
                        return "LEFT JOIN ";
                    default:
                        return "!!!ERRORJOIN!!!";
                }
            }
        }

        internal List<TableJoinType> tableJoinType { get { return _tableJoinType; } }
        
        //Indexer for join type list by relation name
        internal TableJoinType this[string RelationName]
        {
            get
            {
                TableJoinType t;
                t = _tableJoinType.FirstOrDefault(x => x.sTableRelation == RelationName);
                return t;
            }
        }
        private List<TableJoinType> _tableJoinType = new List<TableJoinType>();
        private DataSet dsTablesStruct;
        private DataSet dsTables;

        public DataRelationCollection Relations 
        {
            get { return dsTables.Relations; }            
        }

        public TablesStructure()
        {
            dsTablesStruct = new DataSet();
            dsTables = new DataSet();
        }

        public DataSet TablesStruct
        { get { return dsTablesStruct; } }

        public DataSet Tables
        { get { return dsTables; } }

        public void AddTable(string TableName)
        {
            try
            {
                DataTable dt=new DataTable(TableName);
                StringBuilder sb = new StringBuilder();
                SqlParameter param;
                string sPrimaryKey;

                sb.Append ("SELECT distinct c.name 'Column Name', t.Name 'Data type', c.max_length 'Max Length', c.precision, c.scale,c.is_nullable, ");
                sb.AppendLine ("ISNULL(i.is_primary_key, 0) 'Primary Key'");
                sb.AppendLine ("FROM sys.columns c ");
                sb.AppendLine ("INNER JOIN sys.types t ON c.user_type_id = t.user_type_id ");
                sb.AppendLine ("left outer JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id ");
                sb.AppendLine ("left outer JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id ");
                sb.AppendLine ("WHERE c.object_id = OBJECT_ID(@Table_Name)");

                param = new SqlParameter("@Table_Name", TableName);                

                dt = SqlDAC.ExecuteDataset(ConnectionString, CommandType.Text, sb.ToString(), param).Tables[0];
                dt.TableName = TableName;
                dsTablesStruct.Tables.Add(dt.Copy());
                DataRow[] foundRows;
                foundRows = dt.Select("[Primary Key] = 1");
                //TODO
                sPrimaryKey = foundRows[0][0].ToString();

                sb = new StringBuilder();
                sb.Append ("SELECT * from " + TableName + " where " + sPrimaryKey + "=-1");
                dt=new DataTable(TableName);
                
                dt = SqlDAC.ExecuteDataset(ConnectionString, CommandType.Text, sb.ToString(), param).Tables[0];
                dt.TableName = TableName;
                dsTables.Tables.Add(dt.Copy());
            }
            catch (Exception ex)
            {                
                throw ex;
            }            
        }

        public void LinkTables(string Table1, string Column1, string Table2, string Column2, JoinType joinType=JoinType.InnerJoin)
        {
            try
            {
                string sRelationKey = Table1 + "." + Column1 + "-" + Table2 + "." + Column2;
                if(dsTables.Tables[Table1].Columns[Column1] == null)
                    throw new Exception("שדה " + Column1.ToString() + " לא קיים בטבלה" + Table1.ToString());
                if (dsTables.Tables[Table2].Columns[Column2] == null)
                    throw new Exception("שדה " + Column2.ToString() + " לא קיים בטבלה" + Table2.ToString());
                dsTables.Relations.Add(sRelationKey, dsTables.Tables[Table1].Columns[Column1], dsTables.Tables[Table2].Columns[Column2]);
                tableJoinType.Add(new TableJoinType { joinType, sRelationKey });
            }
            catch (Exception ex)
            {                
                throw ex;
            }
        }
    }
}
