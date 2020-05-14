using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Collections;
using System.Text.RegularExpressions;
using System.Xml.Serialization;

namespace IgalDAL
{
    public class igDataColumn : DataColumn
    {        
        public string MyTableName { get; set; }
        public string ColumnDesc { get { return base.Caption; } set { base.Caption = value; } }
        public string NameColumn { get { return base.ColumnName; } set { base.ColumnName = value; } }
        public string ColumnCondition { get; set; }
        public bool IsDefault { get; set; }
        //public string DataType { get; set; }
        public bool IsVisible { get; set; }

        public override string ToString()
        {
            return this.Caption;
        }

        #region ctor
        public igDataColumn(string _ColumnName)
        { ColumnName = _ColumnName; }

        public igDataColumn(string _TableName, string _ColumnName)
        {
            ColumnName = _ColumnName;
            MyTableName = _TableName;
        }
        #endregion

        public string SqlType
        {
            get
            {   return PublicModule.ParseSystemTypeToSqlType(base.DataType);   }
        }

        public string VarFromCondition(bool WithVarSign)
        {
            string sParamName;
            int iLen = ColumnCondition.Length;
            int iLeft = ColumnCondition.IndexOf('@');
            int iParamNameLen = iLen - iLeft - 1;
            sParamName = ColumnCondition.Substring(iLeft + ((WithVarSign) ? 0 : 1), iParamNameLen + ((WithVarSign) ? 1 : 0));
            return sParamName;
        }
    }

    [Serializable]
    public class igTable : DataTable
    {
        [XmlElement("QueryName")] 
        //public new string TableName { get; set; }

        [XmlAttribute("Alias")]
        public string TableAlias { get; set; }
        [XmlAttribute("NoLock")]
        public bool WithNoLock { get; set; }

        #region Contstructor
        public igTable(string sTableName)
        {
            TableName = sTableName;
            TableAlias = "";
            WithNoLock = true;
        }

        public igTable(string sTableName, string sTableAlias, bool bWithNoLock)
        {
            TableName = sTableName;
            TableAlias = sTableAlias;
            WithNoLock = bWithNoLock;
        }
        #endregion
    }

    /// <summary>
    /// shows relevant select and/or where columns for each defined table
    /// </summary>
    [Serializable]
    public class igQry
    {
        public igQryList Parent { get; set; }

        public enum ColumnType
        {
            SelectColumns,
            WhereColumns
        }

        [XmlElement("SelectColumns")] 
        private List<igDataColumn> _SelectColumns = new List<igDataColumn>();
        [XmlElement("WhereColumns")] 
        private List<igDataColumn> _WhereColumns = new List<igDataColumn>();
        private List<igTable> _Tables = new List<igTable>();

        private string sDeclares="", sSelect="", sFrom="", sGroup="", sWhere="", sHaving="";

        private struct TablePair
        {
            internal string Table1;
            internal string Table2;
        }

        private List<TablePair> PossibleTableJoins = new List<TablePair>();

        public string Declares
        {
            get { return sDeclares; }
            set { sDeclares = Regex.Replace(value, "\r\n\r\n", "\r\n"); }
        }

        public string SelectColumns
        {
            get 
            {
                if (sSelect == "")
                    sSelect = GetSelect();
                return sSelect;                                
            }
            set { sSelect = value; }
        }

        public string TableJoins
        {
            get 
            { 
                if(sFrom == "")
                    sFrom = BuildTableLinkage();
                return sFrom; }
            set { sFrom = value; }
        }

        public string Grouping
        {
            get { return sGroup; }
            set { sGroup = value; }
        }

        public string  FilterWhere
        {
            get { return sWhere; }
            set { sWhere = value; }
        }

        public string FilterHaving
        {
            get { return sHaving; }
            set { sHaving = value; }
        }

        //Indexer for table list by table name
        public igTable this[string criteria]    // Indexer declaration        
        {
            get
            {
                igTable to;
                to = _Tables.FirstOrDefault(tb => tb.TableName == criteria);
                if (to == null)
                    to = _Tables.FirstOrDefault(tb => tb.TableAlias == criteria);
                return to;
            }

        }

        internal string GetSelect()
        {
            string sColumns = "";

            try
            {
                sColumns = GetColumns(ColumnType.SelectColumns);
            }
            catch (Exception)
            {
                sColumns = "*";
            }

            return "select " + sColumns;
        }

        /// <summary>
        /// default MainTable is 1st table added to Tables collection
        /// </summary>
        public string MainTable
        {
            get
            {
                igTable t = Tables.ElementAt(0);
                return t.TableName;
            }
        }

        public List<igTable> Tables { get { return _Tables; } }

        public List<igDataColumn> RelevantSelectColumns
        { get { return _SelectColumns; } }

        public List<igDataColumn> RelevantWhereColumns
        { get { return _WhereColumns; } }

        public void AddSelectColumn(string ColumnName, string ColumnDesc = "", bool isVisible = true)
        {

            igDataColumn aCol = new igDataColumn(ColumnName);
            aCol.Caption = (ColumnDesc.Trim().Length != 0) ? ColumnDesc : ColumnName;
            aCol.IsVisible = isVisible;
            _SelectColumns.Add(aCol);
        }

        public void RemoveSelectColumn(string _ColumnName)
        { _SelectColumns.RemoveAll(item => item.NameColumn == _ColumnName); }

        public void AddWhereColumn(string ColumnName, string Condition, string DataType = "", string ColumnDesc = "", bool _IsDefault = true)
        {
            igDataColumn aCol = new igDataColumn(ColumnName);
            aCol.ColumnCondition = Condition;
            if (DataType != "")
                aCol.DataType = PublicModule.ParseSqlTypeToSystemType(DataType);
            aCol.IsDefault = _IsDefault;
            aCol.Caption = (ColumnDesc.Trim().Length != 0) ? ColumnDesc : ColumnName;
            _WhereColumns.Add(aCol);
        }

        public void RemoveWhereColumn(string _ColumnName)
        { _WhereColumns.RemoveAll(item => item.NameColumn == _ColumnName); }        

        private string GetColumnsCSV(List<string> _ColumnList)
        { return string.Join(", ", from item in _ColumnList select item.ToString()); }

        internal string GetColumns(ColumnType columnType)
        {
            string sCols;

            switch (columnType)
            {
                case ColumnType.SelectColumns:
                    //AddAliasToDuplicateColumn(_SelectColumns,"");
                    sCols = GetColumnsCSV(_SelectColumns.ConvertAll(item => item.NameColumn));
                    break;
                case ColumnType.WhereColumns:
                    //AddAliasToDuplicateColumn(_WhereColumns,"");
                    sCols = GetColumnsCSV(_WhereColumns.ConvertAll(item => item.NameColumn));
                    break;
                default:
                    sCols = "*";
                    break;
            }

            return (sCols.Trim() == "") ? "*" : sCols;
        }

        public string BuildTableLinkage()
        {
            string sFinal = "";
            string sLinkTables = "";
            List<DataRelation> rels;

            TablesStructure _TableStructure = this.Parent.TableStructure;

            try
            {
                BuildPossibleTableJoins();                

                List<TablePair> dupPossible = new List<TablePair>(PossibleTableJoins);

                for (int i = 0; i < this.Tables.Count; i++)
                {
                    igTable tbl = this.Tables[i];
                    string sRelationIndex;
                    if (i > 0)
                    {
                        PossibleTableJoins = new List<TablePair>(dupPossible);

                        foreach (TablePair item in PossibleTableJoins)
                        {                            
                            if (item.Table1 == tbl.TableName | item.Table2 == tbl.TableName)
                            {
                                int iCurJoinCount = 0;
                                rels = GetRealtionsByTable(item.Table1, item.Table2);
                                foreach(DataRelation rel in rels)
                                {
                                    sLinkTables = "";
                                    sRelationIndex = rel.RelationName;
                                    TablesStructure.TableJoinType tj;
                                    tj = _TableStructure.tableJoinType.FirstOrDefault(x => x.sTableRelation == sRelationIndex);
                                    //sFinal += "\r\n" + (_TableStructure.tableJoinType[sRelationIndex].typeJoin==TablesStructure.JoinType.LeftJoin)?"left join":"inner join ";                        
                                    if (iCurJoinCount == 0)
                                    {
                                        sFinal += "\r\n" + tj.ToString();
                                        sLinkTables = " on ";
                                        AddAliasToDuplicateColumn(sRelationIndex);
                                        sFinal += tbl.TableName + " " + tbl.TableAlias + ((tbl.WithNoLock) ? " with(NoLock) " : "");
                                    }
                                    else
                                    {
                                        sLinkTables = " AND ";
                                    }

                                    sLinkTables += this[_TableStructure.Relations[sRelationIndex].ParentColumns[0].Table.TableName].TableAlias + ".";
                                    sLinkTables += _TableStructure.Relations[sRelationIndex].ParentColumns[0].ColumnName;
                                    sLinkTables += " = ";
                                    sLinkTables += this[_TableStructure.Relations[sRelationIndex].ChildColumns[0].Table.TableName].TableAlias + ".";
                                    sLinkTables += _TableStructure.Relations[sRelationIndex].ChildColumns[0].ColumnName;                                                                        

                                    iCurJoinCount++;
                                    sFinal += ((iCurJoinCount == 0) ? " AND " : "") + sLinkTables;

                                    dupPossible.Remove(item);
                                }
                            }
                        }
                    }
                    else
                        sFinal += tbl.TableName + " " + tbl.TableAlias + ((tbl.WithNoLock) ? " with(NoLock) " : "");
                }
            }
            catch (Exception ex)
            { throw ex; }

            return "from " + sFinal;
        }

        private void BuildPossibleTableJoins()
        {
            TablePair pair;
            for (int i = 0; i < this.Tables.Count-1; i++)
            {
                for (int j = i+1; j < this.Tables.Count; j++)
                {
                    pair.Table1 = this.Tables[i].TableName;
                    pair.Table2 = this.Tables[j].TableName;
                    PossibleTableJoins.Add(pair);
                }
            }
        }

        private List<DataRelation> GetRealtionsByTable(string Table1, string Table2)
        {
            TablesStructure _TableStructure = this.Parent.TableStructure;

            List<DataRelation> rels = new List<DataRelation>();

            foreach (DataRelation rel in _TableStructure.Relations)
            {
                if (rel.RelationName.Contains(Table1) && rel.RelationName.Contains(Table2))
                    rels.Add(rel);
            }
            return rels;
        }

        private DataRelation GetRealtionByColumn(string sCol, string RelatedTablesKey)
        {
            TablesStructure _TableStructure = this.Parent.TableStructure;
            bool bChild, bParent;            
            int iHyphenPos = RelatedTablesKey.IndexOf("-");
            string Table1 = RelatedTablesKey.Substring(0, iHyphenPos);
            string Table2 = RelatedTablesKey.Substring(iHyphenPos + 1, RelatedTablesKey.Length - iHyphenPos - 1);

            foreach (DataRelation rel in _TableStructure.Relations)
            {
                if (rel.RelationName.Contains(Table1) && rel.RelationName.Contains(Table2))
                {
                    bChild = rel.ChildColumns.Any(c => c.ColumnName == sCol);
                    bParent = rel.ParentColumns.Any(c => c.ColumnName == sCol);
                    if (bParent && bChild)
                        return rel;
                }
            }
            return null;
        }

        //if a column has the same name in 2 tables and is linked in those tables - add the alias name of 1 of them, e.g. Con_type -> ppd.Con_type
        private void AddAliasToDuplicateColumn(string RelatedTablesKey)
        {
            DataRelation rel;
            foreach (igDataColumn ac in this.RelevantSelectColumns)
            {
                rel = GetRealtionByColumn(ac.ColumnName, RelatedTablesKey);
                if (rel != null)
                    ac.ColumnName = this[rel.ParentTable.TableName].TableAlias + "." + ac.ColumnName;
            }
        }

        private void AddAliasToDuplicateColumn(List<igDataColumn> _SelectColumns, string RelatedTablesKey)
        {
            DataRelation rel;
            foreach (igDataColumn ac in _SelectColumns)
            {
                rel = GetRealtionByColumn(ac.ColumnName, RelatedTablesKey);
                if (rel != null)
                    ac.ColumnName = this[rel.ParentTable.TableName].TableAlias + "." + ac.ColumnName;
            }
        }

        public string GetFullSql()
        {
            //כרגע הוספת אליאס נעשית בתוך קישור טבלאות לכן יש לבצע אותו קודם
            string sFrom = TableJoins;
            string sSelect = SelectColumns;
            
            StringBuilder sSql = new StringBuilder();

            sSql.AppendLine(Declares);
            if (!sSql.ToString().EndsWith("\r\n\r\n"))
                sSql.AppendLine();
            sSql.AppendLine(sSelect);
            sSql.AppendLine(sFrom);
            sSql.AppendLine(FilterWhere);
            sSql.AppendLine(Grouping);
            sSql.AppendLine(FilterHaving);            
            
            return sSql.ToString().Trim();
        }

        //public void ResetQry()
        //{
        //    _WhereColumns.Clear();
        //    _SelectColumns.Clear();
        //    sDeclares = ""; sSelect = ""; sFrom = ""; sGroup = ""; sWhere = ""; sHaving = "";
        //}
    }

    public class igQryList : IEnumerable<igQry>
    {
        List<igQry> _myList = new List<igQry>();
        private TablesStructure _TableStructure = new TablesStructure();

        public TablesStructure TableStructure
        {
            get { return _TableStructure; }
            set { _TableStructure = value; }
        }

        public void AddQry(igQry qry)
        {
            TablesList.Add(qry);
            qry.Parent = this;
        }

        //טבלאות שמשתתפות בשאילתא
        public List<igQry> TablesList
        {
            get
            {                return _myList;            }
        }

        public igQry this[string TableName]
        {
            get
            {
                return _myList.First(item => item.MainTable == TableName);
            }
        }

        //public string GetSelect(igQry hlp)
        //{
        //    //hlp.RelevantSelectColumns 
        //    return hlp.GetSelect();
        //}



        #region technical
        public IEnumerator<igQry> GetEnumerator()
        {
            return _myList.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
        #endregion
    }
}
