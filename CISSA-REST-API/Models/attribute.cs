using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ASIST_REPORT_REST_API.Models
{
    public class attribute
    {
        public Guid id { get; set; }
        public string name { get; set; }
        public string caption { get; set; }
        public string type { get; set; }
        public Guid enumDef { get; set; }
        public string value { get; set; }
        public string enumValueText { get; set; }
    }
}