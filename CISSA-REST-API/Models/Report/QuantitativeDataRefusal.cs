using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ASIST_REPORT_REST_API.Models.Report
{
    public class QuantitativeDataRefusal
    {
        public string district { get; set; }
        public DateTime DateTo { get; set; }
        public int QuantitativeAppCountB { get; set; }
        public string djamoat { get; set; }
        public DateTime DateFormation { get; set; }
        public DateTime DateFrom { get; set; }
    }
}