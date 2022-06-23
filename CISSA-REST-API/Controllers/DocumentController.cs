using ASIST_REPORT_REST_API.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Http.Description;
using System.IO;
using Intersoft.CISSA.DataAccessLayer.Model.Workflow;
using Intersoft.CISSA.DataAccessLayer.Model.Context;

namespace ASIST_REPORT_REST_API.Controllers
{
    public class DocumentController : ApiController
    {
        private sodEntities _sod = new sodEntities();
        [HttpGet]
        [ResponseType(typeof(ScriptExecutor.FamilyDetails))]
        public IHttpActionResult GetFamilyDetailsByIIN(string applicantIIN)
        {
            var log = new RequestLog
            {
                ConnectionName = "GetFamilyDetailsByIIN",
                RequestDate = DateTime.Now,
                Result = "OK"
            };
            try
            {
                var result = ScriptExecutor.GetFamilyDetailsByIIN(applicantIIN);
                if (result == null)
                {
                    log.Result = "Гражданин не найден - " + applicantIIN;
                    return Ok(new { result = false, error = "Гражданин не найден - " + applicantIIN });
                }
                return Ok(new { result = true, data = result });
            }
            catch (Exception e)
            {
                log.Result = "Error: " + e.GetBaseException().Message;
                return Ok(new { result = false, error = e.GetBaseException().Message });
            }
            finally
            {
                log.EllapsedTime = (int)Math.Round((DateTime.Now - log.RequestDate.Value).TotalMilliseconds, 0);
                _sod.RequestLogs.Add(log);
                _sod.SaveChanges();
            }
        }
        [HttpGet]
        [ResponseType(typeof(ScriptExecutor.FamilyDetails))]
        public IHttpActionResult GetFamilyDetailsByIINWithAssign(string applicantIIN)
        {
            var log = new RequestLog
            {
                ConnectionName = "GetFamilyDetailsByIIN",
                RequestDate = DateTime.Now,
                Result = "OK"
            };
            try
            {
                var result = ScriptExecutor.GetFamilyDetailsByIIN(applicantIIN);
                if (result == null)
                {
                    log.Result = "Гражданин не найден - " + applicantIIN;
                    return Ok(new { result = false, error = "Гражданин не найден - " + applicantIIN });
                }
                ScriptExecutor.AssignService(new ScriptExecutor.AssignServiceRequest
                {
                    pin = applicantIIN,
                    serviceTypeId = new Guid("{6EA2082A-D3E9-49F0-836F-F5BE775251BD}"),
                    disabilityGroup = "",
                    amount = 0,
                    djamoat = "",
                    raionNo = 0,
                    oblastNo = 0
                });
                return Ok(new { result = true, data = result });
            }
            catch (Exception e)
            {
                log.Result = "Error: " + e.GetBaseException().Message;
                return Ok(new { result = false, error = e.GetBaseException().Message });
            }
            finally
            {
                log.EllapsedTime = (int)Math.Round((DateTime.Now - log.RequestDate.Value).TotalMilliseconds, 0);
                _sod.RequestLogs.Add(log);
                _sod.SaveChanges();
            }
        }

        [HttpGet]
        [ResponseType(typeof(ScriptExecutor.FamilyDetails))]
        public IHttpActionResult GetFamilyDetailsBySIN(string applicantSIN)
        {
            var log = new RequestLog
            {
                ConnectionName = "GetFamilyDetailsBySIN",
                RequestDate = DateTime.Now,
                Result = "OK"
            };
            try
            {
                var result = ScriptExecutor.GetFamilyDetailsBySIN(applicantSIN);
                if (result == null) log.Result = "Гражданин не найден - " + applicantSIN;
                return Ok(new { result = result != null, error = "Гражданин не найден - " + applicantSIN });
            }
            catch (Exception e)
            {
                log.Result = "Error: " + e.GetBaseException().Message;
                return Ok(new { result = false, error = e.GetBaseException().Message });
            }
            finally
            {
                log.EllapsedTime = (int)Math.Round((DateTime.Now - log.RequestDate.Value).TotalMilliseconds, 0);
                _sod.RequestLogs.Add(log);
                _sod.SaveChanges();
            }
        }

        [HttpPost]
        public IHttpActionResult AssignService([FromBody]ScriptExecutor.AssignServiceRequest request)
        {
            try
            {
                ScriptExecutor.AssignService(request);
                return Ok(new { success = true });
            }
            catch (Exception e)
            {
                return Ok(new { success = false, error = e.GetBaseException().Message });
            }
        }

        static void WriteLog(object text)
        {
            using (StreamWriter sw = new StreamWriter("c:\\distr\\cissa\\asist-rest-api.log", true))
            {
                sw.WriteLine(text.ToString());
            }
        }


        [HttpGet]
        public IHttpActionResult Report_24([FromUri] int year)
        {
            try
            {
                return Ok(ScriptExecutor.Report_24.Execute(year));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        public IHttpActionResult Report_23([FromUri] int year)
        {
            try
            {
                return Ok(ScriptExecutor.Report_23.Execute(year));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        public IHttpActionResult Report_104([FromUri] DateTime fd, [FromUri] DateTime ld)
        {
            try
            {
                return Ok(ScriptExecutor.Report_104.Execute(fd,ld));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        public IHttpActionResult Report_104a([FromUri] int year, [FromUri] int month)
        {
            try
            {
                return Ok(ScriptExecutor.Report_104a.Execute(year, month));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        public IHttpActionResult Report_105a([FromUri] DateTime fd, [FromUri] DateTime ld,  Guid districtId, Guid? djamoatId = null)
        {
            try
            {
                return Ok(ScriptExecutor.Report_105a.Execute(fd, ld, districtId, djamoatId));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        public IHttpActionResult Report_105b([FromUri] DateTime fd, [FromUri] DateTime ld, Guid districtId, Guid? djamoatId = null)
        {
            try
            {
                return Ok(ScriptExecutor.Report_105b.Execute(fd, ld, districtId, djamoatId));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        public IHttpActionResult Report_106([FromUri] DateTime fd, [FromUri] DateTime ld, Guid districtId, Guid? djamoatId = null)
        {
            try
            {
                return Ok(ScriptExecutor.Report_106.Execute(fd, ld, districtId, djamoatId));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        public IHttpActionResult Report_111([FromUri] DateTime fd, [FromUri] DateTime ld, Guid districtId, Guid? djamoatId = null)
        {
            try
            {
                return Ok(ScriptExecutor.Report_111.Execute(fd, ld, districtId, djamoatId));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        public IHttpActionResult Report_112([FromUri] DateTime fd, [FromUri] DateTime ld, Guid districtId, Guid? djamoatId = null)
        {
            try
            {
                return Ok(ScriptExecutor.Report_112.Execute(fd, ld, districtId, djamoatId));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        public IHttpActionResult Report_113([FromUri] DateTime fd, [FromUri] DateTime ld, Guid districtId, Guid? djamoatId = null)
        {
            try
            {
                return Ok(ScriptExecutor.Report_113.Execute(fd, ld, districtId, djamoatId));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        public IHttpActionResult Report_114([FromUri] DateTime fd, [FromUri] DateTime ld, Guid districtId, Guid? djamoatId = null)
        {
            try
            {
                return Ok(ScriptExecutor.Report_114.Execute(fd, ld, districtId, djamoatId));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        public IHttpActionResult Report_115([FromUri] int year, int month)
        {
            try
            {
                return Ok(ScriptExecutor.Report_115.Execute(year, month));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        public IHttpActionResult Report_116([FromUri] DateTime dateFormation)
        {
            try
            {
                return Ok(ScriptExecutor.Report_116.Execute(dateFormation));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }
        [HttpGet]
        public IHttpActionResult Report_117([FromUri] DateTime dateFormation)
        {
            try
            {
                return Ok(ScriptExecutor.Report_117.Execute(dateFormation));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        public IHttpActionResult Report_10g(/*[FromUri] DateTime fd, [FromUri] DateTime ld*/)
        {
            try
            {
                return Ok(ScriptExecutor.Report_10g.Execute(/*fd, ld*/));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        public IHttpActionResult Report_10v([FromUri] DateTime fd, [FromUri] DateTime ld)
        {
            try
            {
                return Ok(ScriptExecutor.Report_10v.Execute(fd, ld));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }
        [HttpGet]
        public IHttpActionResult GetStatisticDataInput([FromUri] int year, int month)
        {
            try
            {
                return Ok(ScriptExecutor.GetStatisticDataInput.Execute(year, month));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }

        [HttpGet]
        public IHttpActionResult Converter(Guid reportId, string json)
        {
            try
            {
                return Ok(ScriptExecutor.Converter.GetSQLString(reportId, json));
            }
            catch (Exception e)
            {
                return BadRequest(e.GetBaseException().Message);
            }
        }
    }
}
