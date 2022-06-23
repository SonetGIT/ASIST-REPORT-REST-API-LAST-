using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ASIST_REPORT_REST_API.Models.Report
{
    public class Refuse
    {
        public int ChildrenUnder_16 { get; set; }
        public QuantitativeDataRefusal quantitativeDataRefusal { get; set; }
        public int FamilyMembers { get; set; }
        public int NumberCases { get; set; }
        public string district { get; set; }
        public int Women_Heads_Family { get; set; }
        public int Unemployed { get; set; }
        public string djamoat { get; set; }
        public int LonelyElderly { get; set; }
        public int DisabledChild { get; set; }
        public int Disabled { get; set; }
        public int ChildrenDisabilities { get; set; }
        public int DisabledGroup_1 { get; set; }
        public Refuse()
        {
            quantitativeDataRefusal = new QuantitativeDataRefusal();
        }
    }
}