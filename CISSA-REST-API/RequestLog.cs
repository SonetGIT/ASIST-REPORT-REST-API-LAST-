//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated from a template.
//
//     Manual changes to this file may cause unexpected behavior in your application.
//     Manual changes to this file will be overwritten if the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace ASIST_REPORT_REST_API
{
    using System;
    using System.Collections.Generic;
    
    public partial class RequestLog
    {
        public int Id { get; set; }
        public string ConnectionName { get; set; }
        public Nullable<System.DateTime> RequestDate { get; set; }
        public Nullable<long> EllapsedTime { get; set; }
        public string Result { get; set; }
    }
}
