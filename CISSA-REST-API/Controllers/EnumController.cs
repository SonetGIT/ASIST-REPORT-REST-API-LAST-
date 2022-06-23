using ASIST_REST_API.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Http.Description;

namespace ASIST_REST_API.Controllers
{
    public class EnumController : ApiController
    {
        private Guid developerUserId = new Guid("{DCED7BEA-8A93-4BAF-964B-232E75A758C5}");
        [HttpGet]
        [ResponseType(typeof(EnumItem[]))]
        public IHttpActionResult GetEnumItems(Guid enumDefId)
        {
            try
            {
                var result = ScriptExecutor.GetEnumItems(enumDefId, developerUserId).Select(x => new EnumItem { Id = x.Id, Text = x.Value });
                return Ok(result);
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }
    }
    public class EnumItem
    {
        public Guid Id { get; set; }
        public string Text { get; set; }
    }
}