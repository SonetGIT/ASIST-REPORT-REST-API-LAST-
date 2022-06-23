using Domain.ResponseTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ASIST_REPORT_REST_API.Models
{
    public class AdoptedChildrenReportItem : IAdoptedChildrenReportItem
    {
        public int No { get; set; }
        public string Name { get; set; }
        public int Boys { get; set; }
        public int Girls { get; set; }
        public int Total { get; set; }
    }
}