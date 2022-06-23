using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ASIST_REPORT_REST_API.Models
{
    public class document
    {
        public Guid id { get; set; }

        public attribute[] attributes { get; set; }
    }
}