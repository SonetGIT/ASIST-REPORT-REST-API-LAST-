using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ASIST_REPORT_REST_API.Models
{
    public class District
    {
        public int Area { get; set; }
        public string Name { get; set; }
        public Guid DistrictType { get; set; }
        public Guid District2 { get; set; }
        public int Number { get; set; }
        public Bank bank { get; set; }

        public District()
        {
            bank = new Bank();
        }
    }
}