using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IgalDAL
{
    public class ScriptNormalizer
    {
        enum StatesEnum
        {
            Code,
            Slash,
            MultiLineComment,
            Asterik,
            Hyphen,
            SingleLineComment
        }
        string sFullScript = "", sNormalizedScript = "";
        StringBuilder sbBuffer = new StringBuilder();
        int iCurrentPosition = 0;        
        int iCreatePosition = -1, iOrPosition = -1;

        public ScriptNormalizer(string FullScript)
        {
            sFullScript = FullScript;            
        }

        /// <summary>
        /// get script /wo comments and remove last spaces from the end of each line
        /// </summary>
        /// <returns></returns>
        public string GetNormalizedScript()
        {
            string[] separator = new string[1] { (Environment.NewLine) };
            string[] lines = sFullScript.Split(separator, StringSplitOptions.RemoveEmptyEntries);
            
            foreach (string line in lines)
            {
                StatesEnum state = StatesEnum.Code;
                string sTrimmed = line.Trim();
                for (int i = 0; i < sTrimmed.Length; i++)
                {
                    char current = sTrimmed[i];
                    switch (state)
                    {
                        case StatesEnum.Code:
                            switch (current)
                            {
                                case '/':
                                    state = StatesEnum.Slash;
                                    break;
                                case '-':
                                    state = StatesEnum.Hyphen;
                                    break;
                                default:
                                    state = StatesEnum.Code;
                                    sNormalizedScript += current;
                                    break;
                            }
                            break;
                        case StatesEnum.Slash:
                            switch (current)
                            {
                                case '*':
                                    state = StatesEnum.Asterik;
                                    break;
                                default:
                                    state = StatesEnum.Code;
                                    sNormalizedScript += @"/" + current;
                                    break;
                                    //return null; //invalid state because there shouldn't be a state in which '/' found /wo asteriks after in the start of the script
                            }
                            break;
                        case StatesEnum.MultiLineComment:
                            switch (current)
                            {
                                case '*':
                                    state = StatesEnum.Asterik;
                                    break;
                                default:
                                    break;
                            }
                            break;
                        case StatesEnum.Asterik:
                            switch (current)
                            {
                                case '/':
                                    state = StatesEnum.Code;
                                    break;
                                case '*':
                                    state = StatesEnum.Asterik;
                                    break;
                                default:
                                    state = StatesEnum.MultiLineComment;
                                    break;
                            }
                            break;
                        case StatesEnum.Hyphen:
                            switch (current)
                            {
                                case '-':
                                    state = StatesEnum.SingleLineComment;
                                    break;
                                default:
                                    state = StatesEnum.Code;
                                    sNormalizedScript += "-" + current;
                                    break;
                                    //return null; //invalid state because there shouldn't be a state in which '-' found /wo another '-' after in the start of the script                                
                            }
                            break;
                        case StatesEnum.SingleLineComment:
                            switch (current)
                            {
                                case '\n':
                                    state = StatesEnum.Code;
                                    break;
                                default:
                                    break;
                            }
                            break;
                        default:
                            throw new Exception($"unknow state {state.ToString()}");
                    }
                }
                sNormalizedScript += Environment.NewLine;
            }            

            return sNormalizedScript.Trim();
        }        
    }
}
