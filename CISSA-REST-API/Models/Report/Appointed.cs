using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ASIST_REPORT_REST_API.Models.Report
{
    public class Appointed
    {
        public string district { get; set; }
        public int WomenHeadsFamily { get; set; }
        public int ChildrenUn16 { get; set; }
        public int NumberFM { get; set; }
        public int LonelyElderly { get; set; }
        public string djamoat { get; set; }
        public int NumberOfApplications { get; set; }
        public int Unemployed { get; set; }
        public QuantitativeData quantitativeData { get; set; }
        public int ChildrenDisability { get; set; }
        public int OverAge { get; set; }
        public int Children_Disability { get; set; }
        public int Over_18_Years { get; set; }
        public Appointed()
        {
            quantitativeData = new QuantitativeData();
        }
    }
}