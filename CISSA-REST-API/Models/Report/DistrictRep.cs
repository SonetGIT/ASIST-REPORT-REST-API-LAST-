using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ASIST_REPORT_REST_API.Models.Report
{
    public class DistrictRep
    {
        public string district { get; set; }
        public GeneralInfoBenefits generalInfoBenefits { get; set; }
        public decimal TotalAmount { get; set; }
        public decimal PercentServices { get; set; }
        public decimal Amount_1 { get; set; }
        public int Number_1 { get; set; }
        public int Number_3 { get; set; }
        public decimal Amount_3 { get; set; }
        public int Number_4 { get; set; }
        public decimal Amount_4 { get; set; }
        public decimal AmountAll { get; set; }
        public int NumberAll { get; set; }
        public decimal Amount_2 { get; set; }
        public int Number_2 { get; set; }
        public DistrictRep()
        {
            generalInfoBenefits = new GeneralInfoBenefits();
        }
    }
}