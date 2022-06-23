using System.Web;
using System.Web.Mvc;

namespace ASIST_REPORT_REST_API
{
    public class FilterConfig
    {
        public static void RegisterGlobalFilters(GlobalFilterCollection filters)
        {
            filters.Add(new HandleErrorAttribute());
        }
    }
}
