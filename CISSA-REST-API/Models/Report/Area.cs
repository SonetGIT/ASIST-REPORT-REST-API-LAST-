using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ASIST_REPORT_REST_API.Models
{
    public class Area
    {
        public Guid Region { get; set; }
        public string Name { get; set; }
        public int Number { get; set; }
        public int Order { get; set; }
    }
}