using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Web.Http;
using Microsoft.Owin.Security.OAuth;
using Newtonsoft.Json.Serialization;

namespace ASIST_REPORT_REST_API
{
    public static class WebApiConfig
    {
        public static void Register(HttpConfiguration config)
        {
            config.EnableCors();
            // Web API configuration and services
            // Configure Web API to use only bearer token authentication.
            config.SuppressDefaultHostAuthentication();
            config.Filters.Add(new HostAuthenticationFilter(OAuthDefaults.AuthenticationType));

            // Web API routes

            config.MapHttpAttributeRoutes();

            /*config.Routes.MapHttpRoute(
                name: "GetDistrictsApi",
                routeTemplate: "api/{controller}/{action}/{regionId}",
                defaults: new { regionId = RouteParameter.Optional }
            );

            config.Routes.MapHttpRoute(
                name: "GetCitiesApi",
                routeTemplate: "api/{controller}/{action}/{districtId}",
                defaults: new { districtId = RouteParameter.Optional }
            );

            config.Routes.MapHttpRoute(
                name: "GetSettlementsApi",
                routeTemplate: "api/{controller}/{action}/{districtId}",
                defaults: new { districtId = RouteParameter.Optional }
            );

            config.Routes.MapHttpRoute(
                name: "GetVillagesApi",
                routeTemplate: "api/{controller}/{action}/{settlementId}",
                defaults: new { settlementId = RouteParameter.Optional }
            );*/


            config.Routes.MapHttpRoute(
                name: "DefaultApi",
                routeTemplate: "api/{controller}/{action}/{id}",
                defaults: new { id = RouteParameter.Optional }
            );

            config.Formatters.JsonFormatter.SupportedMediaTypes.Add(new MediaTypeHeaderValue("text/html"));
            config.Formatters.JsonFormatter.SerializerSettings.ReferenceLoopHandling = Newtonsoft.Json.ReferenceLoopHandling.Ignore;
        }
    }
}
