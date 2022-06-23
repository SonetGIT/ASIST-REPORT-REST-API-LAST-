using ASIST_REST_API.Models;
using ASIST_REST_API.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Http;
using System.Web.Http.Description;

namespace ASIST_REST_API.Controllers
{
    public class MenuController : ApiController
    {
        public IEnumerable<DAL.MenuItem> Get()
        {
            return DAL.GetMenuItems();
        }
    }
}