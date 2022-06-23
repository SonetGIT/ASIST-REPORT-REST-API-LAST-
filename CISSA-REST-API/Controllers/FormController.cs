using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Web.Http;

namespace ASIST_REST_API.Controllers
{
    public class FormController : ApiController
    {
        [HttpGet]
        public HttpResponseMessage GetFormById([FromUri] Guid id)
        {
            try
            {
                var files = Directory.GetFiles(System.Configuration.ConfigurationManager.AppSettings["FormFolderPath"]);
                var formPath = "";
                foreach(var fPath in files)
                {
                    var file = new FileInfo(fPath);
                    if(fPath.ToUpper().Contains(id.ToString().ToUpper()))
                    {
                        formPath = fPath;
                        break;
                    }
                }
                if (File.Exists(formPath))
                {
                    var fileContent = System.IO.File.ReadAllText(formPath);
                    return new HttpResponseMessage() { Content = new StringContent(fileContent, Encoding.UTF8, "application/xml") };
                }
                throw new ApplicationException("File \"" + formPath + "\" doesn't exist in the form folder. Sources: " + string.Join(",", files));
            }
            catch (Exception e)
            {
                throw e;
            }
        }
    }
}