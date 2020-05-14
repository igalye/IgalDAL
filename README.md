# IgalDAL
<h3>SqlDAC</h3> <i>static class</i> - has different functions like ExecuteReader, or ExecuteDataSet, ExecuteScalar - 
for 1-time quick access an MS-SQL DB, including OutputFromScript property - for getting the print commands from console,
ParseSql function 

<h3>clsBaseConnection</h3> - abstract class for multiple us

<h3>clsSimpleField</h3> - has 2 properties - FieldName and FieldValue
<h3>clsParamField</h3> - a bit more complicate class that returns a value with assigning sign - 
ToString(bool bIsSqlValueType) getting the output in format:
FieldName = FieldValue, or
FiledName <= FieldValue.

Possible values of AssignSign property '=','<','<=','>','>=','like'

Another override is 
ToString(igVar_StyleEnum s), where igVar_StyleEnum is Enum :
SimpleStyle,
SqlVariableStyle_SetValue,
SqlVariableStyle_Declare,
SqlVariableStyle_DeclareAndSet

i.e., ToString (SqlVariableStyle_DeclareAndSet) will give the output:
declare @var int = 1

function ValueForScript will quote a given FieldValue (char, string, date, datetime)

<h3>SqlFields</h3> is  IEnumerable<clsParamField> which has also a GetItemByFieldName function - 
a search within a collection of added values by FieldName

<h3>clsSqlBuild</h3> - a class that has a SqlFields vars, a TableName prop, and SqlInsertTypeSelect - select or values.
A final BuildInsert function will return an output with insert like
INSERT INTO table (field1, field2, field3) values (val1,val2,val3)

<h3>clsSqlFilter</h3> builds a where-statement, with logical add default as "AND", though can be changed.
A BuildDeclaresFromFilter function - gets the @-style variables from a filter and builds a declare  
