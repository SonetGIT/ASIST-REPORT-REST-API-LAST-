using ASIST_REST_API.Models.Address;
using ASIST_REST_API.Util;
using System;
using System.Web.Http;
using System.Web.Http.Description;

namespace ASIST_REST_API.Controllers
{
    public class AddressController : ApiController
    {
        //private Guid developerUserId = new Guid("{DCED7BEA-8A93-4BAF-964B-232E75A758C5}");
        [HttpGet]
        [ResponseType(typeof(region[]))]
        public IHttpActionResult GetRegions([FromUri]Guid userId)
        {
            try
            {
                var result = ScriptExecutor.GetRegions(userId);
                return Ok(result);
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        [ResponseType(typeof(district[]))]
        public IHttpActionResult GetDistricts([FromUri] Guid? regionId, [FromUri]Guid userId)
        {
            try
            {
                var result = ScriptExecutor.GetDistricts(userId, regionId);
                return Ok(result);
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        [ResponseType(typeof(city[]))]
        public IHttpActionResult GetCities([FromUri] Guid? districtId, [FromUri]Guid userId)
        {
            try
            {
                var result = ScriptExecutor.GetCities(userId, districtId);
                return Ok(result);
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        [ResponseType(typeof(settlement[]))]
        public IHttpActionResult GetSettlements([FromUri] Guid? districtId, [FromUri]Guid userId)
        {
            try
            {
                var result = ScriptExecutor.GetSettlements(userId, districtId);
                return Ok(result);
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        [ResponseType(typeof(village[]))]
        public IHttpActionResult GetVillages([FromUri] Guid? settlementId, [FromUri]Guid userId)
        {
            try
            {
                var result = ScriptExecutor.GetVillages(userId, settlementId);
                return Ok(result);
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }
    }
}