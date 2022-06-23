using ASIST_REST_API.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Http.Cors;
using System.Web.Http.Description;
using static ASIST_REST_API.Models.DAL;

namespace ASIST_REST_API.Controllers
{
    [EnableCors(origins: "*", headers: "*", methods: "*")]
    public class UsersController : ApiController
    {
        
        [HttpGet]
        [ResponseType(typeof(_cissa_user[]))]
        public IHttpActionResult GetAll()
        {
            try
            {
                return Ok(DAL.GetCissaUsers().ToArray());
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        [ResponseType(typeof(_cissa_user[]))]
        public IHttpActionResult GetOldAll()
        {
            try
            {
                return Ok(DAL.GetOldCissaUsers().ToArray());
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        [ResponseType(typeof(Guid))]
        public IHttpActionResult Login([FromUri] string username, [FromUri] string password)
        {
            try
            {
                var userObj = DAL.GetOldCissaUsers().FirstOrDefault(x => x.UserName.ToLower() == username.ToLower() && x.Password == password);
                if(userObj == null) userObj = DAL.GetCissaUsers().FirstOrDefault(x => x.UserName.ToLower() == username.ToLower() && x.Password == password);
                if (userObj != null)
                {
                    return Ok(new { userId = userObj.Id, orgName = userObj.OrgName });
                }
                else
                {
                    return Ok(new { userId = "", orgName = "", errorMessage = "Unauthorized" });
                }
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }
    }
}