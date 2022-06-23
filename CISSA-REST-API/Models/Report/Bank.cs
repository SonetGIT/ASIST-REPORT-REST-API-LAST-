using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ASIST_REPORT_REST_API.Models
{
    public class Bank
    {
        public string Name { get; set; }
        public string Address { get; set; }
        public string BankCode { get; set; }
        public string AccountNo { get; set; }
        public double PercentServices { get; set; }
    }
}