using Domain.ResponseTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ASIST_REPORT_REST_API.Models
{
    public class AdoptedChildrenReportResponse : IAdoptedChildrenReportResponse
    {
        public IAdoptedChildrenReportItem[] ByAge { get; set; }
        public IAdoptedChildrenReportItem[] ByNationalities { get; set; }
        public IAdoptedChildrenReportItem[] ByGeography { get; set; }
    }
}