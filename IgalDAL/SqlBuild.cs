using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using System.Data.SqlClient;
using System.Text.RegularExpressions;

namespace IgalDAL
{
    public enum igVar_StyleEnum
    {
        SimpleStyle,
        SqlVariableStyle_SetValue,
        SqlVariableStyle_Declare,
        SqlVariableStyle_DeclareAndSet
    }

    /// <summary>
    /// Field with name and value only
    /// </summary>
    public class clsSimpleField
    {
        public string FieldName { get; set; }
        public string FieldValue { get; set; }

        public clsSimpleField()
        {        }

        public clsSimpleField(string _Name, string _Value)
        {
            FieldName = _Name;
            FieldValue = _Value;            
        }

        public override string ToString()
        {
            return FieldName;
        }
    }

    //igal 22/1/20
    public class SqlFieldType
    {
        private string _sqlFieldLength = "";
        
        public string sqlFieldType { get; set; }

    /// <summary>
        /// only numbers, "," or "max" are allowed
        /// </summary>
        public string sqlFieldLength
        {
            get { return _sqlFieldLength; }
            set
            {
                if (!(value == "max" || MatchForPrecision(value)))
                {
                    throw new Exception("only numbers, ',' or 'max' values are allowed ");
                }
                else
                    _sqlFieldLength = value;
            }
        }

        public override string ToString()
        {
            string sPrecision = "";
            if (_sqlFieldLength.Trim() != "")
                sPrecision = "(" + _sqlFieldLength + ")";
            return sqlFieldType + sPrecision;
        }

        private bool MatchForPrecision(string inputstring)
        {
            Regex regex = new Regex(@"[0-9,]");
            MatchCollection matches = regex.Matches(inputstring);

            return matches.Count.Equals(inputstring.Length);
        }
    }
    

    /// <summary>
    /// Field with name, value and type (system style)
    /// </summary>
    public class clsParamField : clsSimpleField
    {
        private string _AssignSign;
        public Type FieldType { get; set; }

        //igal 22/1/20
        public SqlFieldType sqlFieldType
        { get; set; }

        public clsParamField()
        {   //28-1-19
            AssignSign = " = ";
        }

        public clsParamField(string _Name, string _Value):this()
        {
            FieldName = _Name;
            FieldValue = _Value;            
        }

        public clsParamField(string _Name, string _Value, Type _Type):this(_Name,_Value)
        {            FieldType = _Type;        }

        public clsParamField(string _Name, string _Value, SqlFieldType _sqlFieldType) : this(_Name, _Value)
        { sqlFieldType = _sqlFieldType; }

        public override string ToString()
        {
            return FieldName + AssignSign + FieldValue;
        }

        /// <summary>
        /// when true and field type is string or date - adds a single quote before and after the value
        /// </summary>
        /// <param name="bIsSqlValueType"></param>
        /// <returns></returns>
        public string ToString(bool bIsSqlValueType)
        {            
            if(bIsSqlValueType)
                return FieldName + AssignSign + ValueForScript(true);
            else
                return FieldName + AssignSign + FieldValue;
             
            
        }

        public string AssignSign
        {
            get { return _AssignSign; }
            //16-9-19 check for valid signs
            set
            {

                switch (value.Replace(" ", "").ToLower())
                {
                    case "=":
                    case "<":
                    case "<=":
                    case ">":
                    case ">=":
                    case "like":
                    case "in":
                        _AssignSign = value;
                        break;
                    default:
                        throw new Exception("Invalid assign sign. Possible values '=','<','<=','>','>=','like'");
                }
            }
        }

        /// <summary>
        /// returns field name and value in different styles
        /// </summary>
        /// <param name="s">simple style: Field=Value;  SetValue; Declare var; Set and Declare</param>
        /// <returns></returns>
        public string ToString(igVar_StyleEnum s)
        {
            if (FieldName == null)
                return "";

            bool bAddAtSign = !FieldName.Contains('@');
            string AtSign = (bAddAtSign) ? "@" : "";
            switch (s)
            {
                case igVar_StyleEnum.SimpleStyle:
                default:
                    return FieldName + AssignSign + FieldValue;
                case igVar_StyleEnum.SqlVariableStyle_SetValue:
                    return "set " + AtSign + FieldName + AssignSign + FieldValue;
                case igVar_StyleEnum.SqlVariableStyle_Declare:
                    return "declare " + AtSign + FieldName + " " + PublicModule.ParseSystemTypeToSqlType(FieldType);
                case igVar_StyleEnum.SqlVariableStyle_DeclareAndSet:
                    return "declare " + AtSign + FieldName + " " + PublicModule.ParseSystemTypeToSqlType(FieldType) + " = " + ValueForScript(true);
            }
        }

        /// <summary>
        /// quote string or date type value. 
        /// </summary>
        /// <param name="ValueOnly">if a value contains signs like "="(equal) or ">"(more) etc - removes it</param>
        /// <returns>string</returns>
        public string ValueForScript(bool ValueOnly=false)
        {
            if (ValueOnly && FieldValue != null)
            {
                FieldValue=FieldValue.Replace("=","");
                FieldValue = FieldValue.Replace(">", "");
                FieldValue = FieldValue.Replace("<", "");
                FieldValue = FieldValue.Replace("!", "");
            }
            if (FieldType != null)
            {
                switch (FieldType.Name.ToLower())
                {
                    case "char":
                    case "string":
                    case "date":
                    case "datetime":
                        return PublicModule.QuoteString(FieldValue);
                    default:
                        return FieldValue;
                }
            }
            else
                return FieldValue;
        }
    }

    public class SqlFields : IEnumerable<clsParamField>
    {
        private Dictionary<string, clsParamField> mCol;

        public clsParamField Add(String sqlFieldName, SqlFieldType sqlFieldType, String SqlFieldValue, string sKey = "")
        {
            //check if there's already a member with this key
            if (sKey != "" && mCol.ContainsKey(sKey))
                return null;
            //create a new object
            clsParamField objNewMember = new clsParamField(sqlFieldName, SqlFieldValue, sqlFieldType);

            if (sKey.Trim() == "")
                mCol.Add("i" + mCol.Count, objNewMember);
            else
                mCol.Add(sKey, objNewMember);

            return objNewMember;
        }

        public clsParamField Add(String sqlFieldName, String SqlFieldValue, string sKey="" ) 
        {
            //check if there's already a member with this key
            if (sKey != "" && mCol.ContainsKey(sKey))
                return null;
            //create a new object
            clsParamField objNewMember = new clsParamField(sqlFieldName, SqlFieldValue);

            if (sKey.Trim() == "")
                mCol.Add("i" + mCol.Count, objNewMember);
            else
                mCol.Add(sKey, objNewMember);
    
            return objNewMember;    
        }

        public clsParamField this [string vntIndexKey]
        {
            get {
                try
                {
                    return mCol[vntIndexKey.ToString()];
                }
                catch (Exception ex)
                {
                    if (ex.HResult == 1)
                        throw ex;
                    else
                        return null;
                } 
            }
        set
            {                mCol[vntIndexKey.ToString()] = value;            }
        }

        //29-1-19
        public clsParamField GetItemByFieldName(string FieldName)
        {
            clsParamField tmp = null;
            try
            {
            tmp = mCol.First(p => p.Value.FieldName == FieldName).Value;
            }
            catch (Exception ex)
            {
                if (ex.HResult == -2146233079) //Sequence contains no matching element - didn't find an element
                    return null;
                else
                    throw ex;
            }
            
            return tmp;
        }

        public int Count {get {return mCol.Count;}}

        public void Remove(object vntIndexKey)
        {            mCol.Remove (vntIndexKey.ToString());        }

        public SqlFields()
        {            mCol = new Dictionary<string,clsParamField>();        }


        public IEnumerator<clsParamField> GetEnumerator()
        {            
            return mCol.Values.GetEnumerator();            
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();            
        }
    }

    public class clsSqlBuild
    {
        private SqlFields mvarSqlFields ;
        private SqlFields mvarWhere;
        private string mvarTableName; //local variable(s) to hold property value(s)
        //type - Values Or Select
        private bool mvarSqlInsertTypeSelect=true;

        public clsSqlBuild()
        {
            mvarSqlFields = new SqlFields();
            mvarWhere = new SqlFields();            
        }

        public bool SqlInsertTypeSelect 
        { get {return mvarSqlInsertTypeSelect;} set {mvarSqlInsertTypeSelect = value;}  }

        public string TableName 
        { get {return mvarTableName ;} set {mvarTableName =value; }    }

        
        public SqlFields sqlFields
        {
            get
            {                return mvarSqlFields;                    }
            set            {        mvarSqlFields = value;    }
        }

        public string BuildInsert()
        {            
            String sFields ="" , sValues = "";            
            String sInsertType ;
            String sLParanght , sRParanght ;            
            
            try
            {
                int i=0;
                foreach (clsParamField sqlField in mvarSqlFields)
                {
                    i++;
                    sFields += ((i > 1) ? "," : "") + sqlField.FieldName;
                    sValues += ((i > 1) ? "," : "") + sqlField.ValueForScript(true);
                }

                sLParanght = (mvarSqlInsertTypeSelect == false) ? " (" : " ";
                sRParanght = (mvarSqlInsertTypeSelect == false) ? " )" : " ";
                sInsertType = (mvarSqlInsertTypeSelect == true) ? "SELECT" : "VALUES";

                return "INSERT INTO " + mvarTableName + " (" + sFields + " ) " + sInsertType + sLParanght + sValues + sRParanght;
            }
            catch (Exception ex)
            {                
                throw ex;
            }            
        }

    }

    public class clsSqlFilter 
    {
        SqlFields mCol;
        string mvarFilterKey ="";

        public string SQLLogicalAdd
        {        get;        set;            }

        //Put here a words like ""Where"" or just leave empty
        public string FilterKey        
        {get;set;}

        //if the SelectedFields are empty - use all existing field criterias
        public string FinalFilter(bool IncludeFilterKey=true, params object[] SelectFields) 
        {
            int WhereCount =0; 
            string sCriteria ="";            
            bool bAllFields =false;

            if (SelectFields==null) 
                bAllFields = true;
            
            foreach (clsParamField sqlField in mCol)
            {
                if(sqlField.FieldValue != "" )
                {                    
                    WhereCount++;
                    if (WhereCount == 1) sCriteria = " " + mvarFilterKey + " " + sCriteria;
                    if (WhereCount > 1)  sCriteria += " " + SQLLogicalAdd + " ";
                    sCriteria += sqlField.FieldName + " " + sqlField.FieldValue;
                }
            }        
        return ((IncludeFilterKey==true)?(FilterKey):"") + sCriteria;
        }

        //28-1-1
        public string FinalFilterAssign(bool IncludeFilterKey = true, params object[] SelectFields)
        {
            
            int WhereCount = 0;
            string sCriteria = "";
            bool bAllFields = false;
            igVar_StyleEnum varStyle = igVar_StyleEnum.SimpleStyle;

            if (SelectFields == null)
                bAllFields = true;

            foreach (clsParamField sqlField in mCol)
            {
                if (sqlField.FieldValue != "")
                {
                    WhereCount++;
                    if (WhereCount == 1) sCriteria = " " + sCriteria;
                    if (WhereCount > 1) sCriteria += " " + SQLLogicalAdd + " ";
                    //sCriteria += sqlField.FieldName + " " + sqlField.FieldValue;
                    sCriteria += sqlField.ToString(varStyle);
                }
            }
            return ((IncludeFilterKey == true) ? (FilterKey) : "") + sCriteria;
        }

        public string BuildDeclaresFromFilter()
        {
            StringBuilder sDeclares = new StringBuilder();
            IEnumerable<clsParamField> ColsWithParam = mCol.Where(x => x.FieldValue.Contains('@'));
            
            foreach (clsParamField fld in ColsWithParam)
            {
                sDeclares.AppendLine(fld.ToString(igVar_StyleEnum.SqlVariableStyle_DeclareAndSet));
            }
            return sDeclares.ToString();
        }

        public SqlFields sqlFields
        {
            get
            { return mCol; }
            set { mCol = value; }
        }

        public clsSqlFilter()
        {
            mCol = new SqlFields();
            //default AND
            SQLLogicalAdd = " AND ";
        }
    }

}
