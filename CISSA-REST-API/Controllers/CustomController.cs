using ASIST_REST_API.Models;
using ASIST_REST_API.Util;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Web.Http;
using System.Web.Http.Description;

namespace ASIST_REST_API.Controllers
{
    public class CustomController : ApiController
    {
        [HttpPost]
        [ResponseType(typeof(MSECDetails))]
        public IHttpActionResult MSECDetails([FromBody] PINRequest request)
        {
            try
            {
                var result = ScriptExecutor.GetMSECDetails(request);
                return Ok(result);
            }
            catch(Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        [ResponseType(typeof(AdoptedChildrenReportResponse))]
        public IHttpActionResult AdoptedChildren(/*[FromBody] YearMonthRequest request*/)
        {
            try
            {
                var result = AdoptedChildrenReport.Execute();
                if (result != null)
                    return Ok(result);
                else
                    return NotFound();
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }
    }
}
