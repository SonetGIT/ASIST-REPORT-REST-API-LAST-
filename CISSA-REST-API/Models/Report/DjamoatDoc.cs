using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ASIST_REPORT_REST_API.Models
{
    public class DjamoatDoc
    {
        public District district { get; set; }
        public string Name { get; set; }
        public Guid DistrictType { get; set; }
        public DjamoatDoc()
        {
            district = new District();
        }
    }
}