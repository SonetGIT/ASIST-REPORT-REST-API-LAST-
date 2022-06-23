using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Intersoft.CISSA.BizService.Utils;
using Intersoft.CISSA.DataAccessLayer.Core;
using Intersoft.CISSA.DataAccessLayer.Model.Context;
using Intersoft.CISSA.DataAccessLayer.Model.Query.Builders;
using Intersoft.CISSA.DataAccessLayer.Model.Workflow;
using Intersoft.CISSA.DataAccessLayer.Model.Query.Sql;
using Intersoft.CISSA.DataAccessLayer.Model.Data;
using Intersoft.CISSA.DataAccessLayer.Model.Documents;
using Intersoft.CISSA.DataAccessLayer.Repository;

using ASIST_REPORT_REST_API.Models;
using Intersoft.CISSA.DataAccessLayer.Model.Enums;
using Intersoft.CISSA.DataAccessLayer.Model.Query;
using ASIST_REPORT_REST_API.Models.Address;
using System.IO;
using System.Data;
using System.Reflection;
using ASIST_REPORT_REST_API.Models.Report;
using System.Runtime.Serialization;
using Newtonsoft.Json;

namespace ASIST_REPORT_REST_API.Util
{
    public static class ScriptExecutor
    {
        public static WorkflowContext CreateAsistContext(string username, Guid userId)
        {
            var dataContextFactory = DataContextFactoryProvider.GetFactory();

            var dataContext = dataContextFactory.CreateMultiDc("asistDataContexts");
            BaseServiceFactory.CreateBaseServiceFactories();
            var providerFactory = AppServiceProviderFactoryProvider.GetFactory();
            var provider = providerFactory.Create(dataContext);
            var serviceRegistrator = provider.Get<IAppServiceProviderRegistrator>();
            serviceRegistrator.AddService(new UserDataProvider(userId, username));
            return new WorkflowContext(new WorkflowContextData(Guid.Empty, userId), provider);
        }
        public static WorkflowContext CreateNrszContext(string username, Guid userId)
        {
            var dataContextFactory = DataContextFactoryProvider.GetFactory();

            var dataContext = dataContextFactory.CreateMultiDc("nrszDataContexts");
            BaseServiceFactory.CreateBaseServiceFactories();
            var providerFactory = AppServiceProviderFactoryProvider.GetFactory();
            var provider = providerFactory.Create(dataContext);
            var serviceRegistrator = provider.Get<IAppServiceProviderRegistrator>();
            serviceRegistrator.AddService(new UserDataProvider(userId, username));
            return new WorkflowContext(new WorkflowContextData(Guid.Empty, userId), provider);
        }

        private static readonly Guid asistPersonDefId = new Guid("{6052978A-1ECB-4F96-A16B-93548936AFC0}");
        static Guid appDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}");
        public class FamilyDetails
        {
            public Person Applicant { get; set; }
            public FamilyMembersDetail FamilyMembers { get; set; }
            public ASPApplication ASPApplication { get; set; }
        }

        public class FamilyMembersDetail
        {
            public int? all { get; set; }
            public int? group_1 { get; set; }
            public int? children_disabilities { get; set; }
            public int? Children_under_16 { get; set; }
            public int? Older_than_65 { get; set; }
        }
        public class ASPApplication
        {
            public string No { get; set; }
            public DateTime? Date { get; set; }
            public DateTime? ApplicationDate { get; set; }
            public Person Trustee { get; set; }
            public int OblastNo { get; set; }
            public string OblastName { get; set; }
            public int RaionNo { get; set; }
            public string RaionName { get; set; }
            public string Djamoat { get; set; }
            public string Village { get; set; }
            public string Street { get; set; }
            public string House { get; set; }
            public string Flat { get; set; }
            public string DisabilityGroup { get; set; }
        }

        public class Person
        {
            public string IIN { get; set; }
            public string SIN { get; set; }
            public string Last_Name { get; set; }
            public string First_Name { get; set; }
            public string Middle_Name { get; set; }
            public Guid? GenderId { get; set; }
            public string GenderText { get; set; }
            public DateTime? Date_of_Birth { get; set; }
            public Guid? PassportTypeId { get; set; }
            public string PassportTypeText { get; set; }
            public string PassportSeries { get; set; }
            public string PassportNo { get; set; }
            public DateTime? Date_of_Issue { get; set; }
            public string Issuing_Authority { get; set; }
        }

        public static FamilyDetails GetFamilyDetailsByIIN(string applicantIIN)
        {
            var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));

            var qb = new QueryBuilder(appDefId);
            qb.Where("Person").Include("IIN").Eq(applicantIIN).End();
            var query = context.CreateSqlQuery(qb);
            var ui = context.GetUserInfo();
            query.AddAttribute("&Id");
            query.AddOrderAttribute("&Created", false);
            var id = Guid.Empty;
            using (var reader = context.CreateSqlReader(query))
            {
                if (reader.Read())
                {
                    id = reader.GetGuid(0);
                }
            }
            if (id != Guid.Empty)
            {
                var model = new FamilyDetails();
                var docRepo = context.Documents;
                var app = docRepo.LoadById(id);
                var person = docRepo.LoadById((Guid)app["Person"]);

                model.Applicant = InitPerson(context, person);
                model.ASPApplication = new ASPApplication
                {
                    ApplicationDate = (DateTime?)app["ApplicationDate"],
                    Date = (DateTime?)app["Date"],
                    No = app["No"] != null ? app["No"].ToString() : "",
                    Trustee = app["Trustee"] != null ? InitPerson(context, docRepo.LoadById((Guid)app["Trustee"])) : null
                };
                if (app["Application_State"] != null)
                {
                    var appState = docRepo.LoadById((Guid)app["Application_State"]);
                    model.FamilyMembers = new FamilyMembersDetail
                    {
                        all = (int?)appState["all"],
                        children_disabilities = (int?)appState["children_disabilities"],
                        Children_under_16 = (int?)appState["Children_under_16"],
                        group_1 = (int?)appState["group_1"],
                        Older_than_65 = (int?)appState["Older_than_65"]
                    };
                    if (appState["Disability"] != null)
                    {
                        model.ASPApplication.DisabilityGroup = context.Enums.GetValue((Guid)appState["Disability"]).Value;
                        if (appState["DisabilityGroupe"] != null)
                        {
                            model.ASPApplication.DisabilityGroup += ", " + context.Enums.GetValue((Guid)appState["DisabilityGroupe"]).Value;
                        }
                    }
                    if (appState["RegionId"] != null)
                    {
                        var regionObj = docRepo.LoadById((Guid)appState["RegionId"]);
                        model.ASPApplication.OblastNo = (int?)regionObj["Number"] ?? 0;
                        model.ASPApplication.OblastName = regionObj["Name"] != null ? regionObj["Name"].ToString() : "";
                    }
                    if (appState["DistrictId"] != null)
                    {
                        var districtObj = docRepo.LoadById((Guid)appState["DistrictId"]);
                        model.ASPApplication.RaionNo = (int?)districtObj["Number"] ?? 0;
                        model.ASPApplication.RaionName = districtObj["Name"] != null ? districtObj["Name"].ToString() : "";
                    }
                    if (appState["DjamoatId"] != null)
                    {
                        var djamoatObj = docRepo.LoadById((Guid)appState["DjamoatId"]);
                        model.ASPApplication.Djamoat = djamoatObj["Name"] != null ? djamoatObj["Name"].ToString() : "";
                    }
                    if (appState["VillageId"] != null)
                    {
                        var villageObj = docRepo.LoadById((Guid)appState["VillageId"]);
                        model.ASPApplication.Village = villageObj["Name"] != null ? villageObj["Name"].ToString() : "";
                    }
                    if (appState["street"] != null)
                    {
                        model.ASPApplication.Street = appState["street"].ToString();
                    }
                    if (appState["House"] != null)
                    {
                        model.ASPApplication.House = appState["House"].ToString();
                    }
                    if (appState["flat"] != null)
                    {
                        model.ASPApplication.Flat = appState["flat"].ToString();
                    }
                }
                return model;
            }
            return null;
        }
        public static FamilyDetails GetFamilyDetailsBySIN(string applicantSIN)
        {
            var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));

            var qb = new QueryBuilder(appDefId, context.UserId);
            qb.Where("Person").Include("SIN").Eq(applicantSIN).End();
            var query = context.CreateSqlQuery(qb);
            query.AddAttribute("&Id");
            query.AddOrderAttribute("&Created", false);
            var id = Guid.Empty;
            using (var reader = context.CreateSqlReader(query))
            {
                if (reader.Read())
                {
                    id = reader.GetGuid(0);
                }
            }
            if (id != Guid.Empty)
            {
                var model = new FamilyDetails();
                var docRepo = context.Documents;
                var app = docRepo.LoadById(id);
                var person = docRepo.LoadById((Guid)app["Person"]);

                model.Applicant = InitPerson(context, person);
                model.ASPApplication = new ASPApplication
                {
                    ApplicationDate = (DateTime?)app["ApplicationDate"],
                    Date = (DateTime?)app["Date"],
                    No = app["No"] != null ? app["No"].ToString() : "",
                    Trustee = app["Trustee"] != null ? InitPerson(context, docRepo.LoadById((Guid)app["Trustee"])) : null
                };
                if (app["Application_State"] != null)
                {
                    var appState = docRepo.LoadById((Guid)app["Application_State"]);
                    model.FamilyMembers = new FamilyMembersDetail
                    {
                        all = (int?)appState["all"],
                        children_disabilities = (int?)appState["children_disabilities"],
                        Children_under_16 = (int?)appState["Children_under_16"],
                        group_1 = (int?)appState["group_1"],
                        Older_than_65 = (int?)appState["Older_than_65"]
                    };
                }

                return model;
            }
            return null;
        }
        static Person InitPerson(WorkflowContext context, Doc person)
        {
            var p = new Person();
            p.IIN = person["IIN"] != null ? person["IIN"].ToString() : "";
            p.SIN = person["SIN"] != null ? person["SIN"].ToString() : "";
            p.Last_Name = person["Last_Name"] != null ? person["Last_Name"].ToString() : "";
            p.First_Name = person["First_Name"] != null ? person["First_Name"].ToString() : "";
            p.Middle_Name = person["Middle_Name"] != null ? person["Middle_Name"].ToString() : "";
            p.GenderId = person["Sex"] != null ? (Guid)person["Sex"] : Guid.Empty;
            p.GenderText = person["Sex"] != null ? context.Enums.GetValue((Guid)person["Sex"]).Value : "";
            p.Date_of_Birth = (DateTime?)person["Date_of_Birth"];
            p.PassportTypeId = person["PassportType"] != null ? (Guid)person["PassportType"] : Guid.Empty;
            p.PassportTypeText = person["PassportType"] != null ? context.Enums.GetValue((Guid)person["PassportType"]).Value : "";
            p.PassportSeries = person["PassportSeries"] != null ? person["PassportSeries"].ToString() : "";
            p.PassportNo = person["PassportNo"] != null ? person["PassportNo"].ToString() : "";
            p.Date_of_Issue = (DateTime?)person["Date_of_Issue"];
            p.Issuing_Authority = person["Issuing_Authority"] != null ? person["Issuing_Authority"].ToString() : "";
            return p;
        }

        public static void SetAssigned(string applicantIIN)
        {
            var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));

            var qb = new QueryBuilder(appDefId, context.UserId);
            qb.Where("Person").Include("IIN").Eq(applicantIIN).End();
            var query = context.CreateSqlQuery(qb);
            query.AddAttribute("&Id");
            query.AddOrderAttribute("&Created", false);
            var id = Guid.Empty;
            using (var reader = context.CreateSqlReader(query))
            {
                if (reader.Read())
                {
                    id = reader.GetGuid(0);
                }
            }
            if (id != Guid.Empty)
            {
                var docRepo = context.Documents;
                var app = docRepo.LoadById(id);
                var person = docRepo.LoadById((Guid)app["Person"]);
                app["RitualBenefitAssigned"] = true;
                docRepo.Save(app);
            }
        }
        private static readonly Guid NrszPersonDefId = new Guid("{6052978A-1ECB-4F96-A16B-93548936AFC0}");
        public class AssignServiceRequest
        {
            public string pin { get; set; }
            public DateTime effectiveDate { get; set; }
            public DateTime expiryDate { get; set; }
            public Guid serviceTypeId { get; set; }
            public double amount { get; set; }
            public int oblastNo { get; set; }
            public int raionNo { get; set; }
            public string djamoat { get; set; }
            public string village { get; set; }
            public string street { get; set; }
            public string house { get; set; }
            public string flat { get; set; }
            public string disabilityGroup { get; set; }
        }
        public static void AssignService(AssignServiceRequest request)
        {
            var nrszContext = CreateNrszContext("asist2nrsz", new Guid("{05EEF54F-5BFE-4E2B-82C7-6AB6CD59D488}"));
            nrszContext.DataContext.BeginTransaction();
            try
            {
                var qb = new QueryBuilder(NrszPersonDefId);
                qb.Where("IIN").Eq(request.pin);
                Guid personId;
                Guid districtId;
                Guid regionId;

                using (var query = nrszContext.CreateSqlQuery(qb.Def))
                {
                    query.AddAttribute("&Id");
                    using (var reader = nrszContext.CreateSqlReader(query))
                    {
                        if (!reader.Read())
                            throw new ApplicationException(
                                String.Format("Не могу зарегистрированить назначение. Гражданин с указанным ПИН \"{0}\" не найден!", request.pin));

                        personId = reader.Reader.GetGuid(0);
                    }
                }
                qb = new QueryBuilder(RaionDefId);
                qb.Where("Number").Eq(request.raionNo).And("Area").Include("Number").Eq(request.oblastNo);

                using (var query = nrszContext.CreateSqlQuery(qb.Def))
                {
                    query.AddAttribute("&Id");
                    query.AddAttribute(query.Sources[0], "&Id");
                    using (var reader = nrszContext.CreateSqlReader(query))
                    {
                        if (!reader.Read())
                            throw new ApplicationException("Не могу зарегистрировать назначение. Ошибка в коде области или района!");

                        districtId = reader.Reader.GetGuid(0);
                        regionId = reader.Reader.GetGuid(1);
                    }
                }
                var docRepo = nrszContext.Documents;
                var assignedService = docRepo.New(AssignedServiceDefId);
                assignedService["Person"] = personId;
                assignedService["RegDate"] = DateTime.Now;
                assignedService["DateFrom"] = request.effectiveDate;
                assignedService["DateTo"] = request.expiryDate;
                if (request.amount > 0)
                    assignedService["Amount"] = request.amount;
                assignedService["ServiceType"] = request.serviceTypeId;
                var userInfo = nrszContext.GetUserInfo();
                assignedService["AuthorityId"] = userInfo.OrganizationId;
                assignedService["District"] = districtId;
                assignedService["Area"] = regionId;
                assignedService["Djamoat"] = request.djamoat;
                assignedService["Village"] = request.village;
                assignedService["Street"] = request.street;
                assignedService["House"] = request.house;
                assignedService["Flat"] = request.flat;

                assignedService["DisabilityGroup"] = request.disabilityGroup;


                nrszContext.Documents.Save(assignedService);
                nrszContext.DataContext.Commit();
            }
            catch (Exception e)
            {
                nrszContext.DataContext.Rollback();
                throw e;
            }
        }

        public static readonly Guid ServiceTypeEnumDefId = new Guid("{EA5A7FC9-19AF-4E18-BF21-E8EE29D585C7}");
        public static readonly Guid TsaBenefitEnumId = new Guid("{C5C95DC9-CEFE-46F5-B6AA-4D23E5CE1008}"); // Пособие на АСП
        public static readonly Guid TsaPoorStatusEnumId = new Guid("{371B3E58-C039-4F8C-A299-B62666C23AB6}"); // Статус малообеспеченной семьи
        public static readonly Guid TsaDeadBenefitEnumId = new Guid("{2A5FA716-7D3A-467B-B7C8-C1E68251E7D4}"); // Пособие на погребение
        public static readonly Guid AssignedServiceDefId = new Guid("{A16EE2A1-CFDF-4B7A-8A32-28CC094C3486}");
        private static readonly Guid RaionDefId = new Guid("{BA5D4276-6BFB-4180-9D4F-828E38E95601}");
        private static readonly Guid AssignedServicePaymentDefId = new Guid("{B9CB0BD2-9BD5-4F91-AD12-94B9FA6FC21D}");

        static void WriteLog(object text)
        {
            using (StreamWriter sw = new StreamWriter("c:\\distr\\cissa\\asist-rest-api.log", true))
            {
                sw.WriteLine(text.ToString());
            }
        }

        public static class Report_24
        {
            public static List<PaymentsByQuarter> Execute(int year)
            {
                var reportItems = new List<PaymentsByQuarter>();
                var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));
                var report = new BudgetFundsQquarterMain();

                if (year < 2014 || year > 3000)
                    throw new ApplicationException("Ошибка в периоде!");
                var items = new List<SItem>();
                var qb = new QueryBuilder(paymentDefId, context.UserId);
                qb.Where("Assignment").Include("Year").Eq(year).End();

                using (var query = SqlQueryBuilder.Build(context.DataContext, qb.Def))
                {
                    var assignMentSrc = query.JoinSource(query.Source, assignmentDefId, SqlSourceJoinType.Inner, "Assignment");
                    var appSource = query.JoinSource(assignMentSrc, AppDefId, SqlSourceJoinType.Inner, "Application");
                    var appStateSource = query.JoinSource(appSource, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");
                    var regDisNoSrc = query.JoinSource(query.Source, RegDistrictNoDefId, SqlSourceJoinType.Inner, "Registry_DistrictNo");

                    query.AddAttribute(appStateSource, "DistrictId");
                    query.AddAttribute(assignMentSrc, "Month");
                    query.AddAttribute(assignMentSrc, "Amount");
                    query.AndCondition(regDisNoSrc, "&State", ConditionOperation.Equal, RegDisNoStateId);

                    using (var reader = new SqlQueryReader(context.DataContext, query))
                    {
                        while (reader.Read())
                        {
                            var districtId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                            var month = reader.GetInt32(1);
                            var amount = reader.GetDecimal(2);
                            items.Add(new SItem { DistrictId = districtId, Month = month, Amount = amount });
                        }
                    }
                }
                foreach (var dId in items.GroupBy(i => i.DistrictId).Select(i => i.Key))
                {
                    var ds = new BudgetFundsQquarter();
                    ds.budgetFundsQquarterMain = report;
                    if (dId != Guid.Empty)
                    {
                        ds.District = dId;
                        ds.DistrictName = context.Documents.LoadById(dId)["Name"].ToString();
                    }
                    var districtAmount = 0m;
                    for (var q = 1; q < 5; q++)
                    {
                        var quarterSum = items.Where(i => i.DistrictId == dId && GetQuarter(i.Month) == q).Sum(i => i.Amount);
                        setValueByProperty(ds, "Amount" + q.ToString(), quarterSum);
                        districtAmount += quarterSum;
                    }
                    ds.TotalAmount = districtAmount;
                    var dItems = items.Where(i => i.DistrictId == dId).ToList();
                    var si = new PaymentsByQuarter();
                    si.budgetFundsQquarter = ds;
                    si.Year = year;

                    var yearAmount = 0m;
                    for (var m = 1; m < 13; m++)
                    {
                        var monthAmount = dItems.Where(i => i.Month == m).Sum(i => i.Amount);
                        int quarterIndex = ((m - 1) / 3) + 1;
                        setValueByProperty(si, "Amount" + m.ToString(), monthAmount, quarterIndex);
                        yearAmount += monthAmount;
                    }
                    si.YearAmount = yearAmount;
                    reportItems.Add(si);
                }
                return reportItems;
            }

            private static BudgetFundsQquarter setValueByProperty(BudgetFundsQquarter item, string propertyName, decimal value)
            {
                if (!string.IsNullOrEmpty(propertyName))
                {
                    PropertyInfo propertyInfo = item.GetType().GetProperty(propertyName);
                    propertyInfo.SetValue(item, value, null);
                }
                return item;
            }

            private static PaymentsByQuarter setValueByProperty(PaymentsByQuarter item, string propertyName, decimal value, int quarterIndex)
            {

                if (!string.IsNullOrEmpty(propertyName))
                {

                    switch (quarterIndex)
                    { case 1:
                            {
                                var quarter = item.quarter1;
                                PropertyInfo propertyInfo = quarter.GetType().GetProperty(propertyName);
                                propertyInfo.SetValue(quarter, value, null);
                                break;
                            }
                        case 2:
                            {
                                var quarter = item.quarter2;
                                PropertyInfo propertyInfo = quarter.GetType().GetProperty(propertyName);
                                propertyInfo.SetValue(quarter, value, null);
                                break;
                            }
                        case 3:
                            {
                                var quarter = item.quarter3;
                                PropertyInfo propertyInfo = quarter.GetType().GetProperty(propertyName);
                                propertyInfo.SetValue(quarter, value, null);
                                break;
                            }
                        case 4:
                            {
                                var quarter = item.quarter4;
                                PropertyInfo propertyInfo = quarter.GetType().GetProperty(propertyName);
                                propertyInfo.SetValue(quarter, value, null);
                                break;
                            }

                    }


                }
                return item;
            }

            public class ReportItem
            {
                public BudgetFundsQquarterMain budgetFundsQquarterMain { get; set; }
            }

            public class BudgetFundsQquarterMain
            {
                public DateTime Date { get; set; }
                public int Year { get; set; }
            }

            public class BudgetFundsQquarter
            {
                public Guid District { get; set; }
                public string DistrictName { get; set; }
                public decimal Amount1 { get; set; }
                public decimal Amount2 { get; set; }
                public decimal Amount3 { get; set; }
                public decimal Amount4 { get; set; }
                public decimal TotalAmount { get; set; }
                public BudgetFundsQquarterMain budgetFundsQquarterMain { get; set; }
            }

            public class PaymentsByQuarter
            {
                public BudgetFundsQquarter budgetFundsQquarter { get; set; }
                public int Year { get; set; }
                public Quarter1 quarter1 { get; set; }
                public Quarter2 quarter2 { get; set; }
                public Quarter3 quarter3 { get; set; }
                public Quarter4 quarter4 { get; set; }
                public decimal YearAmount { get; set; }

                public PaymentsByQuarter()
                {
                    quarter1 = new Quarter1();
                    quarter2 = new Quarter2();
                    quarter3 = new Quarter3();
                    quarter4 = new Quarter4();
                }
            }
            public class Quarter1
            {
                public decimal Amount1 { get; set; }
                public decimal Amount2 { get; set; }
                public decimal Amount3 { get; set; }
            }
            public class Quarter2
            {
                public decimal Amount4 { get; set; }
                public decimal Amount5 { get; set; }
                public decimal Amount6 { get; set; }
            }
            public class Quarter3
            {
                public decimal Amount7 { get; set; }
                public decimal Amount8 { get; set; }
                public decimal Amount9 { get; set; }
            }
            public class Quarter4
            {
                public decimal Amount10 { get; set; }
                public decimal Amount11 { get; set; }
                public decimal Amount12 { get; set; }
            }

            private static int? GetQuarter(int month)
            {
                if (new[] { 1, 2, 3 }.Contains(month))
                    return 1;
                if (new[] { 4, 5, 6 }.Contains(month))
                    return 2;
                if (new[] { 7, 8, 9 }.Contains(month))
                    return 3;
                if (new[] { 10, 11, 12 }.Contains(month))
                    return 4;
                return null;
            }

            private static readonly Guid reportDefId = new Guid("{1A533826-5C9E-4151-8AE7-8CF1FF1D4775}");
            private static readonly Guid reportItemDefId = new Guid("{D0DB009B-EE70-44B9-8D7C-71C30BCF2318}");
            private static readonly Guid itemDefId = new Guid("{5F9409D5-111B-4F3D-94F5-A6275506E246}");

            private static readonly Guid paymentDefId = new Guid("{68667FBB-C149-4FB3-93AD-1BBCE3936B6E}");
            private static readonly Guid assignmentDefId = new Guid("{51935CC6-CC48-4DAC-8853-DA8F57C057E8}");

            public static readonly Guid AppDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}");
            public static readonly Guid AppStateDefId = new Guid("{547BBA55-2281-4388-A1FC-EE890168AC2D}");
            public static readonly Guid BankAccountDefId = new Guid("{BE6D5C1F-48A6-483B-980A-14CEFF781FD4}");
            public static readonly Guid RegDistrictNoDefId = new Guid("{DB434DEC-259F-4563-9213-301D9E38753D}");

            public static readonly Guid AssignedStateId = new Guid("{ACB44CC8-BF44-44F4-8056-723CED22536C}");
            public static readonly Guid OnPaymentStateId = new Guid("{78C294B5-B6EA-4075-9EEF-52073A6A2511}");
            public static readonly Guid RegDisNoStateId = new Guid("{9BCE67C9-DD5D-42BC-9D07-E194CD3A804C}");

            internal class SItem
            {
                public Guid DistrictId { get; set; }
                public int Month { get; set; }
                public decimal Amount { get; set; }
            }
        }

        public static class Report_23
        {
            public static List<ReportBudgetByMonths> Execute(int year)
            {
                var reportItems = new List<ReportBudgetByMonths>();
                var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));
                var report = new PaymentsAreaItem();

                if (year < 2014 || year > 3000)
                    throw new ApplicationException("Ошибка в периоде!");
                var items = new List<SItem>();
                var qb = new QueryBuilder(paymentDefId, context.UserId);
                qb.Where("Assignment").Include("Year").Eq(year).End();

                using (var query = SqlQueryBuilder.Build(context.DataContext, qb.Def))
                {
                    var assignMentSrc = query.JoinSource(query.Source, assignmentDefId, SqlSourceJoinType.Inner, "Assignment");
                    var appSource = query.JoinSource(assignMentSrc, AppDefId, SqlSourceJoinType.Inner, "Application");
                    var appStateSource = query.JoinSource(appSource, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");
                    var regDisNoSrc = query.JoinSource(query.Source, RegDistrictNoDefId, SqlSourceJoinType.Inner, "Registry_DistrictNo");

                    query.AddAttribute(appStateSource, "DistrictId");
                    query.AddAttribute(assignMentSrc, "Month");
                    query.AddAttribute(assignMentSrc, "Amount");

                    query.AndCondition(regDisNoSrc, "&State", ConditionOperation.Equal, RegDisNotateId);

                    using (var reader = new SqlQueryReader(context.DataContext, query))
                    {
                        while (reader.Read())
                        {
                            var districtId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                            var month = reader.GetInt32(1);
                            var amount = reader.GetDecimal(2);
                            items.Add(new SItem { DistrictId = districtId, Month = month, Amount = amount });
                        }
                    }
                }
                foreach (var dId in items.GroupBy(i => i.DistrictId).Select(i => i.Key))
                {
                    var ds = new ReportBudgetByMonths();
                    ds.paymentsAreaItem = report;
                    if (dId != Guid.Empty)
                        ds.district = context.Documents.LoadById(dId)["Name"].ToString();
                    var districtAmount = 0m;
                    for (var m = 1; m < 13; m++)
                    {
                        var monthSum = items.Where(i => i.DistrictId == dId && i.Month == m).Sum(i => i.Amount);
                        ds = setValueByProperty(ds, "Amount_" + m.ToString(), monthSum);
                        districtAmount += monthSum;
                    }
                    ds.TotalAmount = districtAmount;
                    reportItems.Add(ds);

                }
                return reportItems;
            }

            private static ReportBudgetByMonths setValueByProperty(ReportBudgetByMonths item, string propertyName, decimal value)
            {
                if (!string.IsNullOrEmpty(propertyName))
                {
                    PropertyInfo propertyInfo = item.GetType().GetProperty(propertyName);
                    propertyInfo.SetValue(item, value, null);
                }
                return item;
            }

            private static readonly Guid reportDefId = new Guid("{7A44AE8D-E9B2-44C3-95DE-7B7233437BCD}"); //Отчет об использовании бюджетных средств АСП по месяцам
            private static readonly Guid reportItemDefId = new Guid("{185C78DD-5B00-4146-91E5-F6EBF562EBB5}"); //Районы

            private static readonly Guid paymentDefId = new Guid("{68667FBB-C149-4FB3-93AD-1BBCE3936B6E}");
            private static readonly Guid assignmentDefId = new Guid("{51935CC6-CC48-4DAC-8853-DA8F57C057E8}");

            public static readonly Guid AppDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}");
            public static readonly Guid AppStateDefId = new Guid("{547BBA55-2281-4388-A1FC-EE890168AC2D}");
            public static readonly Guid BankAccountDefId = new Guid("{BE6D5C1F-48A6-483B-980A-14CEFF781FD4}");
            public static readonly Guid RegDistrictNoDefId = new Guid("{DB434DEC-259F-4563-9213-301D9E38753D}");

            public static readonly Guid AssignedStateId = new Guid("{ACB44CC8-BF44-44F4-8056-723CED22536C}");
            public static readonly Guid OnPaymentStateId = new Guid("{78C294B5-B6EA-4075-9EEF-52073A6A2511}");
            public static readonly Guid RegDisNotateId = new Guid("{9BCE67C9-DD5D-42BC-9D07-E194CD3A804C}");

            internal class SItem
            {
                public Guid DistrictId { get; set; }
                public int Month { get; set; }
                public decimal Amount { get; set; }
            }

            public class ReportBudgetByMonths
            {
                public PaymentsAreaItem paymentsAreaItem { get; set; }
                public string district { get; set; }
                public decimal Amount_1 { get; set; }
                public decimal Amount_2 { get; set; }
                public decimal Amount_3 { get; set; }
                public decimal Amount_4 { get; set; }
                public decimal Amount_5 { get; set; }
                public decimal Amount_6 { get; set; }
                public decimal Amount_7 { get; set; }
                public decimal Amount_8 { get; set; }
                public decimal Amount_9 { get; set; }
                public decimal Amount_10 { get; set; }
                public decimal Amount_11 { get; set; }
                public decimal Amount_12 { get; set; }
                public decimal TotalAmount { get; set; }

                public ReportBudgetByMonths()
                {
                    paymentsAreaItem = new PaymentsAreaItem();
                }

            }

            public class PaymentsAreaItem
            {
                public DateTime Date { get; set; }
                public int Year { get; set; }
                public string Note { get; set; }
            }

            public class District
            {
                public int Area { get; set; }
                public string Name { get; set; }
                public Guid DistrictType { get; set; }
                public Guid District2 { get; set; }
                public int Number { get; set; }
                public Bank bank { get; set; }

                public District()
                {
                    bank = new Bank();
                }
            }

            public class Area
            {
                public Guid Region { get; set; }
                public string Name { get; set; }
                public int Number { get; set; }
                public int Order { get; set; }
            }

            public class Bank
            {
                public string Name { get; set; }
                public string Address { get; set; }
                public string BankCode { get; set; }
                public string AccountNo { get; set; }
                public double PercentServices { get; set; }
            }

        }
        public static class Report_104
        {
            public static readonly Guid UserId = new Guid("{E0F19306-AECE-477B-B110-3AD09323DD2D}");
            public static readonly string Username = "Admin";
            public static readonly Guid regionId = new Guid("{8c5e9217-59ac-4b4e-a41a-643fc34444e4}");
            public static readonly Guid districtId = new Guid("{4D029337-C025-442E-8E93-AFD1852073AC}");
            public static readonly Guid djamoatId = new Guid("{967D525D-9B76-44BE-93FA-BD4639EA515A}");

            public class Place
            {
                public Guid Id { get; set; }
                public string Name { get; set; }
                public Guid Def { get; set; }
                public decimal transferred { get; set; }
                public decimal paid { get; set; }
                public decimal deposited { get; set; }
                public decimal returned { get; set; }


            }

            [DataContract]
            public class RegionObject
            {
                [DataMember]
                public Guid Id { get; set; }
                [DataMember]
                public string Name { get; set; }
                [DataMember]
                public Guid Def { get; set; }
                [DataMember]
                public decimal transferred { get; set; }
                [DataMember]
                public decimal paid { get; set; }
                [DataMember]
                public decimal deposited { get; set; }
                [DataMember]
                public decimal returned { get; set; }
                [DataMember]
                public List<DistrictObject> districts { get; set; }
                public RegionObject()
                {
                    districts = new List<DistrictObject>();
                }
            }

            [DataContract]
            public class DistrictObject
            {
                [DataMember]
                public Guid Id { get; set; }
                [DataMember]
                public string Name { get; set; }
                [DataMember]
                public string RegionName { get; set; }
                [DataMember]
                public Guid Def { get; set; }
                [DataMember]
                public decimal transferred { get; set; }
                [DataMember]
                public decimal paid { get; set; }
                [DataMember]
                public decimal deposited { get; set; }
                [DataMember]
                public decimal returned { get; set; }
                [DataMember]
                public List<DjamoatObject> djamoats { get; set; }
                public DistrictObject()
                {
                    djamoats = new List<DjamoatObject>();
                }

            }

            [DataContract]
            public class DjamoatObject
            {
                [DataMember]
                public Guid Id { get; set; }
                [DataMember]
                public string Name { get; set; }
                [DataMember]
                public string DistrictName { get; set; }
                [DataMember]
                public Guid Def { get; set; }
                [DataMember]
                public decimal transferred { get; set; }
                [DataMember]
                public decimal paid { get; set; }
                [DataMember]
                public decimal deposited { get; set; }
                [DataMember]
                public decimal returned { get; set; }
            }

            public static List<Place> GetListFromSqlQueryReader(WorkflowContext context, Guid PlaceId, string columnName)
            {
                List<Place> placeList = new List<Place>();
                var docRepo = context.Documents;
                var docDefRepo = context.DocDefs;
                var docDef = docDefRepo.DocDefById(PlaceId);
                var qb = new QueryBuilder(PlaceId, context.UserId);
                var sqlQueryBuilder = context.Get<ISqlQueryBuilder>();
                using (var query = sqlQueryBuilder.Build(qb.Def))
                {
                    var q = query.BuildSql().ToString();
                    docDef.Attributes.ForEach(x =>
                    {
                        query.AddAttribute(x.Name);
                    });
                    //var str = query.BuildSql().ToString().Replace("\t", " ").Replace("\r", " ").Replace("\n", " ").Replace("\"", " ");
                    var table = new DataTable();
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    foreach (DataRow row in table.Rows)
                    {
                        Place place = new Place();
                        place.Id = Guid.Parse(row["Id1"].ToString());
                        place.Name = row["Name"].ToString();
                        place.Def = Guid.Parse(row[columnName].ToString());
                        placeList.Add(place);
                    }
                }
                return placeList;
            }
            public static List<RegionObject> GetAdministrativeDivision()
            {
                List<RegionObject> regionList = new List<RegionObject>();
                List<DjamoatObject> djamoatList = new List<DjamoatObject>();
                List<DistrictObject> districtList = new List<DistrictObject>();
                var context = CreateAsistContext(Username, UserId);
                var areaList = GetListFromSqlQueryReader(context, regionId, "Id1");

                var _districtList = GetListFromSqlQueryReader(context, districtId, "Area");
                var _djamoatList = GetListFromSqlQueryReader(context, djamoatId, "District");
                foreach (var djamoat in _djamoatList)
                {

                    DjamoatObject _djamoat = new DjamoatObject();
                    _djamoat.Id = djamoat.Id;
                    _djamoat.Name = djamoat.Name;
                    _djamoat.Def = djamoat.Def;
                    _djamoat.DistrictName = context.Documents.LoadById(djamoat.Def)["Name"].ToString();
                    _djamoat.transferred = djamoat.transferred;
                    _djamoat.deposited = djamoat.deposited;
                    _djamoat.paid = djamoat.paid;
                    _djamoat.returned = djamoat.returned;
                    djamoatList.Add(_djamoat);

                }
                foreach (var district in _districtList)
                {
                    DistrictObject _district = new DistrictObject();
                    _district.Id = district.Id;
                    _district.Name = district.Name;
                    _district.Def = district.Def;
                    _district.RegionName = context.Documents.LoadById(district.Def)["Name"].ToString();
                    _district.transferred = district.transferred;
                    _district.deposited = district.deposited;
                    _district.paid = district.paid;
                    _district.returned = district.returned;
                    _district.djamoats = djamoatList.Where(x => x.Def.Equals(district.Id)).ToList();
                    districtList.Add(_district);
                }
                foreach (var region in areaList)
                {
                    regionList.Add
                                    (new RegionObject
                                    {
                                        Id = region.Id,
                                        Name = region.Name,
                                        Def = region.Def,
                                        transferred = region.transferred,
                                        paid = region.paid,
                                        deposited = region.deposited,
                                        returned = region.returned,
                                        districts = districtList.Where(x => x.Def.Equals(region.Id)).ToList()
                                    }
                                    );
                }

                return regionList;
            }


            public static List<RegionObject> Execute(DateTime fd, DateTime ld)
            {
                var regionObjectList = GetAdministrativeDivision();

                var financialReport = new FinancialReport();
                var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));
                var year = DateTime.Now.Year;
                var month = DateTime.Now.Month;
                var date = DateTime.Now;
                if (fd == DateTime.MinValue || ld == DateTime.MaxValue || fd >= ld || date == DateTime.MinValue)
                    throw new ApplicationException("Ошибка в периоде!");

                var items = new List<ReportItem>();
                var qb = new QueryBuilder(BankAccTernDefId, context.UserId);
                qb.Where("Report").Include("PeriodFrom").Le(ld).And("PeriodTo").Ge(fd).End();

                using (var query = SqlQueryBuilder.Build(context.DataContext, qb.Def))
                {
                    var bankAccountSrc = query.JoinSource(query.Source, BankAccountDefId, SqlSourceJoinType.Inner, "BankAccount");
                    var appSrc = query.JoinSource(bankAccountSrc, AppDefId, SqlSourceJoinType.Inner, "Application");
                    var appStateSrc = query.JoinSource(appSrc, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");

                    query.AddAttribute(appStateSrc, "RegionId");
                    query.AddAttribute(appStateSrc, "DistrictId");
                    query.AddAttribute(appStateSrc, "DjamoatId");
                    query.AddAttribute(query.Source, "Transferred");
                    query.AddAttribute(query.Source, "Paid");
                    query.AddAttribute(query.Source, "Deposited");
                    query.AddAttribute(query.Source, "Returned");

                    using (var reader = new SqlQueryReader(context.DataContext, query))
                    {
                        while (reader.Read())
                        {
                            var regionId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                            var districtId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                            var djamoatId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                            var transferred = reader.IsDbNull(3) ? 0 : reader.GetDecimal(3);
                            var paid = reader.IsDbNull(4) ? 0 : reader.GetDecimal(4);
                            var deposited = reader.IsDbNull(5) ? 0 : reader.GetDecimal(5);
                            var returned = reader.IsDbNull(6) ? 0 : reader.GetDecimal(6);

                            var item = GetItem(items, regionId, districtId, djamoatId);
                            item.Received = ((decimal?)item.Received ?? 0) + transferred;
                            item.Paid = ((decimal?)item.Paid ?? 0) + paid;
                            item.Deposited = ((decimal?)item.Deposited ?? 0) + deposited;
                            item.Returned = ((decimal?)item.Returned ?? 0) + returned;
                        }
                    }
                }

                var recTotal = 0m;
                var paidTotal = 0m;
                var depTotal = 0m;
                var retTotal = 0m;

                foreach (var regions in items.GroupBy(x => x.RegionId))
                {
                    var financialReportByRegions = new FinancialReportByRegions();
                    financialReportByRegions.DateFormation = date;
                    financialReportByRegions.DateFrom = fd;
                    financialReportByRegions.DateTo = ld;
                    financialReportByRegions.area = context.Documents.LoadById(regions.Key)["Name"].ToString();
                    RegionObject regionObject = regionObjectList.Where(x => x.Id.Equals(regions.Key)).FirstOrDefault();
                    var regRec = 0m;
                    var regPaid = 0m;
                    var regDep = 0m;
                    var regRet = 0m;

                    foreach (var districts in regions.GroupBy(x => x.DistrictId))
                    {
                        var financialReportByDistricts = new FinancialReportByDistricts();
                        financialReportByDistricts.DateFormation = date;
                        financialReportByDistricts.DateFrom = fd;
                        financialReportByDistricts.DateTo = ld;
                        financialReportByDistricts.district = context.Documents.LoadById(districts.Key)["Name"].ToString();
                        DistrictObject districtObject = regionObject.districts.Where(x => x.Id.Equals(districts.Key)).FirstOrDefault();

                        var disRec = 0m;
                        var disPaid = 0m;
                        var disDep = 0m;
                        var disRet = 0m;

                        foreach (var item in districts)
                        {
                            var financialReportByDamoat = new FinancialReportByDamoat();
                            financialReportByDamoat.DateFormation = date;
                            financialReportByDamoat.djamoat = context.Documents.LoadById(item.DjamoatId)["Name"].ToString();
                            DjamoatObject djamoatObject = districtObject.djamoats.Where(x => x.Id.Equals(item.DjamoatId)).FirstOrDefault();

                            var received = item.Received ?? 0;
                            var paid = item.Paid ?? 0;
                            var deposited = item.Deposited ?? 0;
                            var returned = item.Returned ?? 0;

                            financialReportByDamoat.ReceivedDj = received;
                            financialReportByDamoat.PaidDj = paid;
                            financialReportByDamoat.DepositedDj = deposited;
                            financialReportByDamoat.Returned = returned;
                            financialReportByDistricts.financialReportByDamoat.Add(financialReportByDamoat);

                            djamoatObject.transferred = received;
                            djamoatObject.paid = paid;
                            djamoatObject.deposited = deposited;
                            djamoatObject.returned = returned;

                            disRec += received;
                            disPaid += paid;
                            disDep += deposited;
                            disRet += returned;
                        }
                        financialReportByDistricts.ReceivedDis = disRec;
                        financialReportByDistricts.PaidDis = disPaid;
                        financialReportByDistricts.DepositedDis = disDep;
                        financialReportByDistricts.Returned = disRet;
                        financialReportByRegions.financialReportByDistricts.Add(financialReportByDistricts);

                        districtObject.transferred = disRec;
                        districtObject.paid = disPaid;
                        districtObject.deposited = disDep;
                        districtObject.returned = disRet;

                        regRec += disRec;
                        regPaid += disPaid;
                        regDep += disDep;
                        regRet += disRet;
                    }
                    financialReportByRegions.ReceivedReg = regRec;
                    financialReportByRegions.PaidReg = regPaid;
                    financialReportByRegions.DepositedReg = regDep;
                    financialReportByRegions.Returned = regRet;
                    financialReport.financialReportByRegions.Add(financialReportByRegions);

                    regionObject.transferred = regRec;
                    regionObject.paid = regPaid;
                    regionObject.deposited = regDep;
                    regionObject.returned = regRet;

                    recTotal += regRec;
                    paidTotal += regPaid;
                    depTotal += regDep;
                    retTotal += regRet;
                }

                financialReport.Received = recTotal;
                financialReport.Paid = paidTotal;
                financialReport.Deposited = depTotal;
                financialReport.Returned = retTotal;
                return regionObjectList;
            }

            private static readonly Guid reportDefId = new Guid("{A29D11BE-6C84-4889-9428-1B28AAA7F9D3}");
            private static readonly Guid reportItemRegionDefId = new Guid("{4D78411A-7907-4AF5-9099-61DAC8F31B38}"); //по областям
            private static readonly Guid reportItemDistrictDefId = new Guid("{2B248B40-2146-4216-88B2-1F6DA41C4120}"); //по районам
            private static readonly Guid reportItemDjamoatDefId = new Guid("{9848498E-F9EA-407D-B2D0-0543D393F8EF}"); //по джамоатам

            private static readonly Guid BankAccTernDefId = new Guid("{982C14E1-3F9C-4559-8835-314C612AB021}"); //Оборот банковского счета
            private static readonly Guid AppDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}");
            private static readonly Guid BankAccountDefId = new Guid("{BE6D5C1F-48A6-483B-980A-14CEFF781FD4}"); //Банковский счет
            private static readonly Guid AppStateDefId = new Guid("{547BBA55-2281-4388-A1FC-EE890168AC2D}");

            private static readonly Guid OnPaymentStateId = new Guid("{78C294B5-B6EA-4075-9EEF-52073A6A2511}");

            private static ReportItem GetItem(List<ReportItem> items, Guid regionId, Guid districtId, Guid djamoatId)
            {
                var item = items.FirstOrDefault(x => x.RegionId == regionId && x.DistrictId == districtId && x.DjamoatId == djamoatId);
                if (item != null) return item;
                var newItem = new ReportItem { RegionId = regionId, DistrictId = districtId, DjamoatId = djamoatId };
                items.Add(newItem);
                return newItem;
            }
            private class ReportItem
            {
                public Guid RegionId;
                public Guid DistrictId;
                public Guid DjamoatId;
                public decimal? Received;
                public decimal? Paid;
                public decimal? Deposited;
                public decimal? Returned;
            }

            public class FinancialReport
            {
                public DateTime DateFormation { get; set; }
                public DateTime DateFrom { get; set; }
                public DateTime DateTo { get; set; }
                public decimal Received { get; set; }
                public decimal Paid { get; set; }
                public decimal Deposited { get; set; }
                public decimal Returned { get; set; }
                public List<FinancialReportByRegions> financialReportByRegions { get; set; }

                public FinancialReport()
                {
                    financialReportByRegions = new List<FinancialReportByRegions>();
                }
            }

            public class FinancialReportByRegions
            {
                public DateTime DateFormation { get; set; }
                public string area { get; set; }
                public decimal ReceivedReg { get; set; }
                public decimal PaidReg { get; set; }
                public decimal DepositedReg { get; set; }
                public decimal Returned { get; set; }
                public List<FinancialReportByDistricts> financialReportByDistricts { get; set; }
                public DateTime DateFrom { get; set; }
                public DateTime DateTo { get; set; }

                public FinancialReportByRegions()
                {
                    financialReportByDistricts = new List<FinancialReportByDistricts>();
                }
            }

            public class FinancialReportByDistricts
            {
                public string district { get; set; }
                public DateTime DateFormation { get; set; }
                public decimal ReceivedDis { get; set; }
                public DateTime DateFrom { get; set; }
                public DateTime DateTo { get; set; }
                public decimal PaidDis { get; set; }
                public decimal DepositedDis { get; set; }
                public decimal Returned { get; set; }
                public List<FinancialReportByDamoat> financialReportByDamoat { get; set; }

                public FinancialReportByDistricts()
                {
                    financialReportByDamoat = new List<FinancialReportByDamoat>();
                }

            }

            public class FinancialReportByDamoat
            {
                public string djamoat { get; set; }
                public decimal ReceivedDj { get; set; }
                public decimal PaidDj { get; set; }
                public decimal DepositedDj { get; set; }
                public decimal Returned { get; set; }
                public DateTime DateFormation { get; set; }
            }

            public class Area
            {
                public Guid Region { get; set; }
                public string Name { get; set; }
                public int Number { get; set; }
                public int Order { get; set; }
            }

            public class District
            {
                public int Area { get; set; }
                public string Name { get; set; }
                public Guid DistrictType { get; set; }
                public Guid District2 { get; set; }
                public int Number { get; set; }
                public Bank bank { get; set; }

                public District()
                {
                    bank = new Bank();
                }
            }

            public class Bank
            {
                public string Name { get; set; }
                public string Address { get; set; }
                public string BankCode { get; set; }
                public string AccountNo { get; set; }
                public double PercentServices { get; set; }
            }

            public class DjamoatDoc
            {
                public string district { get; set; }
                public string Name { get; set; }
                public Guid DistrictType { get; set; }
            }
        }

        public static class Report_104a
        {
            public static List<DistrictRep> Execute(int year, int month)
            {
                var reportItems = new List<DistrictRep>();
                var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));
                var date = DateTime.Now;
                double precent = 0;
                if (year < 2014 || year > 3000)
                    throw new ApplicationException("Ошибка в периоде!");
                if (month < 1 || month > 12)
                    throw new ApplicationException("Ошибка в значении месяца!");

                var items = new List<SItem>();
                var qb = new QueryBuilder(paymentDefId);
                using (var query = SqlQueryBuilder.Build(context.DataContext, qb.Def))
                {
                    var districtRegSrc = query.JoinSource(query.Source, DistrictBankPaymentDefId, SqlSourceJoinType.Inner, "Registry");
                    var registrySrc = query.JoinSource(districtRegSrc, RegistryDefId, SqlSourceJoinType.Inner, "BankPaymentRegistry");
                    var bankSrc = query.JoinSource(registrySrc, BankDefId, SqlSourceJoinType.Inner, "Bank");
                    var bankAccountSrc = query.JoinSource(query.Source, BankAccountDefId, SqlSourceJoinType.Inner, "BankAccount");
                    var appSource = query.JoinSource(bankAccountSrc, AppDefId, SqlSourceJoinType.Inner, "Application");
                    var appStateSource = query.JoinSource(appSource, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");
                    var assignMentSrc = query.JoinSource(query.Source, AssignmentDefId, SqlSourceJoinType.Inner, "Assignment");
                    var regDisNoSrc = query.JoinSource(query.Source, RegDistrictNoDefId, SqlSourceJoinType.Inner, "Registry_DistrictNo");

                    query.AndCondition(registrySrc, "Year", ConditionOperation.Equal, year);
                    query.AndCondition(registrySrc, "Month", ConditionOperation.Equal, month);
                    query.AndCondition(regDisNoSrc, "&State", ConditionOperation.Equal, RegDisNoStateId);

                    query.AddAttribute(bankSrc, "PercentServices");
                    query.AddAttribute(appStateSource, "DistrictId");
                    query.AddAttribute(assignMentSrc, "No");
                    query.AddAttribute(assignMentSrc, "Month");
                    query.AddAttribute(assignMentSrc, "Amount");
                    query.AddAttribute(query.Source, "&Id", "count({0})");

                    query.AddGroupAttribute(bankSrc, "PercentServices");
                    query.AddGroupAttribute(appStateSource, "DistrictId");
                    query.AddGroupAttribute(assignMentSrc, "No");
                    query.AddGroupAttribute(assignMentSrc, "Month");
                    query.AddGroupAttribute(assignMentSrc, "Amount");

                    query.AddOrderAttribute(bankSrc, "PercentServices");
                    query.AddOrderAttribute(appStateSource, "DistrictId");
                    query.AddOrderAttribute(assignMentSrc, "No");
                    query.AddOrderAttribute(assignMentSrc, "Month");
                    query.AddOrderAttribute(assignMentSrc, "Amount");

                    using (var reader = new SqlQueryReader(context.DataContext, query))
                    {
                        while (reader.Read())
                        {
                            precent = reader.GetDouble(0);
                            var districtId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                            var no = reader.IsDbNull(2) ? 0 : reader.GetInt32(2);
                            var assignMonth = reader.GetInt32(3);
                            var amount = reader.GetDecimal(4);
                            var count = reader.GetInt32(5);
                            items.Add(new SItem { DistrictId = districtId, No = no, Month = assignMonth, Amount = amount, Precent = precent, Count = count });
                        }
                    }
                }
                decimal percentOfService = Convert.ToDecimal(precent);
                foreach (var dId in items.GroupBy(i => i.DistrictId).Select(i => i.Key))
                {
                    var ds = new DistrictRep();
                    if (dId != Guid.Empty)
                        ds.district = context.Documents.LoadById(dId)["Name"].ToString();
                    var districtNumber = 0;
                    var districtAmount = 0m;
                    decimal percentAmount = 0m;
                    var totalAmount = 0m;
                    for (var q = 1; q < 5; q++)
                    {
                        var quarterSum = items.Where(i => i.DistrictId == dId && i.No == q).Sum(i => i.Count);
                        setValueByProperty(ds, "Number_" + q.ToString(), quarterSum);
                        setValueByProperty(ds, "Amount_" + q.ToString(), (decimal)(quarterSum * 100));
                        districtNumber += quarterSum;
                        districtAmount += quarterSum;
                    }
                    ds.NumberAll = districtNumber;
                    ds.AmountAll = districtAmount * 100;
                    ds.PercentServices = districtAmount * 100 * percentOfService / 100;
                    ds.TotalAmount = districtAmount * 100 + districtAmount * 100 * percentOfService / 100;
                    reportItems.Add(ds);
                }
                return reportItems;
            }

            private static int? GetQuarter(int mon)
            {
                if (new[] { 1, 2, 3 }.Contains(mon))
                    return 1;
                if (new[] { 4, 5, 6 }.Contains(mon))
                    return 2;
                if (new[] { 7, 8, 9 }.Contains(mon))
                    return 3;
                if (new[] { 10, 11, 12 }.Contains(mon))
                    return 4;
                return null;
            }
            private static readonly Guid paymentDefId = new Guid("{68667FBB-C149-4FB3-93AD-1BBCE3936B6E}");
            private static readonly Guid DistrictBankPaymentDefId = new Guid("{ADF1D21A-5FCE-4F42-8889-D0714DDF7967}");
            private static readonly Guid BankAccountDefId = new Guid("{BE6D5C1F-48A6-483B-980A-14CEFF781FD4}");
            private static readonly Guid BankDefId = new Guid("{B722BED0-562E-4872-8DD7-ACC31A0C1E12}");//Банк 
            private static readonly Guid RegistryDefId = new Guid("{B3BB3306-C3B4-4F67-98BF-B015DEEDEFFF}");
            private static readonly Guid AssignmentDefId = new Guid("{51935CC6-CC48-4DAC-8853-DA8F57C057E8}");
            private static readonly Guid AppDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}");
            private static readonly Guid AppStateDefId = new Guid("{547BBA55-2281-4388-A1FC-EE890168AC2D}");

            private static readonly Guid reportDefId = new Guid("{8960E633-BF11-45C9-8FAC-06969B254F42}");
            private static readonly Guid reportItemDefId = new Guid("{E489E33C-2011-4DBB-B4A6-277A484CFE66}");

            public static readonly Guid RegDistrictNoDefId = new Guid("{DB434DEC-259F-4563-9213-301D9E38753D}");
            public static readonly Guid RegDisNoStateId = new Guid("{9BCE67C9-DD5D-42BC-9D07-E194CD3A804C}");
            private static readonly Guid RegistryStateId = new Guid("{BA7384FE-895E-462F-8BAB-83CB593CDB08}");

            internal class SItem
            {
                public Guid DistrictId { get; set; }
                public int No { get; set; }
                public int Month { get; set; }
                public decimal Amount { get; set; }
                public int Count { get; set; }
                public double Precent { get; set; }
            }

            private static DistrictRep setValueByProperty(DistrictRep item, string propertyName, decimal value)
            {
                if (!string.IsNullOrEmpty(propertyName))
                {
                    PropertyInfo propertyInfo = item.GetType().GetProperty(propertyName);
                    propertyInfo.SetValue(item, value, null);
                }
                return item;
            }

            private static DistrictRep setValueByProperty(DistrictRep item, string propertyName, int value)
            {
                if (!string.IsNullOrEmpty(propertyName))
                {
                    PropertyInfo propertyInfo = item.GetType().GetProperty(propertyName);
                    propertyInfo.SetValue(item, value, null);
                }
                return item;
            }
        }

        public static class Report_105a
        {
            public static GeneralReportItem Execute(DateTime fd, DateTime ld, Guid districtId, Guid? djamoatId)
            {
                var reportItems = new List<Appointed>();
                var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));
                var generalReportItem = new GeneralReportItem();
                var docRepo = context.Documents;

                var distrId = districtId;
                //var djamId = djamoatId;
                var djamId = (Guid?)djamoatId ?? Guid.Empty;

                if (djamId != Guid.Empty)
                {
                    distrId = (Guid?)docRepo.LoadById(djamId)["District"] ?? Guid.Empty;
                }
                if (fd == DateTime.MinValue || ld == DateTime.MaxValue || ld <= fd)
                    throw new ApplicationException("Ошибка в периоде!");

                var qb = new QueryBuilder(AppDefId, context.UserId);
                qb.Where("Date").Ge(fd).And("Date").Le(ld).And("&State").In(new object[] { OnPaymentStateId, AssignedStateId, ReadyAppointStateId })
                .And("Application_State").Include("BALL").Le(GetBall(context, fd, ld)).End();

                var table = new DataTable();
                using (var query = context.CreateSqlQuery(qb.Def))
                {
                    var appStateSource = query.JoinSource(query.Source, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");
                    if (djamId != Guid.Empty)
                        query.AndCondition(appStateSource, "DjamoatId", ConditionOperation.Equal, djamId);
                    else if (distrId != Guid.Empty)
                        query.AndCondition(appStateSource, "DistrictId", ConditionOperation.Equal, distrId);

                    query.AddAttribute(query.Source, "&Id");
                    query.AddAttribute(appStateSource, "&Id");

                    using (var reader = context.CreateSqlReader(query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                }
                var items = new List<ReportItem>();
                foreach (DataRow row in table.Rows)
                {
                    var app = docRepo.LoadById((Guid)row[0]);
                    var appState = docRepo.LoadById((Guid)row[1]);
                    var district_Id = (Guid?)appState["DistrictId"] ?? Guid.Empty;
                    var djamoat_Id = (Guid?)appState["DjamoatId"] ?? Guid.Empty;

                    var item = GetItem(items, district_Id, djamoat_Id);
                    item.Count = (item.Count ?? 0) + 1;

                    int memCount = 0;
                    int c;
                    foreach (var memId in docRepo.DocAttrList(out c, appState, "Family_Member", 0, 0))
                    {
                        memCount++;
                        item.FemAll = (item.FemAll ?? 0) + 1;
                        var mem = docRepo.LoadById(memId);
                        var mPerson = docRepo.LoadById((Guid)mem["Person"]);
                        var bd = (DateTime?)mPerson["Date_of_Birth"] ?? DateTime.MinValue;
                        if (bd != DateTime.MinValue)
                        {
                            var mAge = fd.Year - (bd.Month < fd.Month ? bd.Year - 1 : bd.Year);
                            if (mAge <= 16)
                                item.Child16 = (item.Child16 ?? 0) + 1;

                            var hasDis = (Guid?)mem["DisablilityGroupe"] ?? Guid.Empty;
                            if (hasDis == FirstGroup)
                            {
                                if (mAge >= 18)
                                    item.Child18 = (item.Child18 ?? 0) + 1;
                                else
                                    item.ChildDis1Gr = (item.ChildDis1Gr ?? 0) + 1;
                            }
                            if (hasDis == SecondGroup || hasDis == ThirdGroup)
                            {
                                if (mAge >= 18)
                                    item.Child18ST = (item.Child18ST ?? 0) + 1;
                                else
                                    item.ChildDis23Gr = (item.ChildDis23Gr ?? 0) + 1;
                            }
                        }
                    }

                    var person = docRepo.LoadById((Guid)app["Person"]);
                    var sex = (Guid?)person["Sex"] ?? Guid.Empty;

                    if (sex == womanSexId)
                    {
                        var familyState = (Guid?)person["FamilyState"] ?? Guid.Empty;
                        if (new List<Guid>
                    {
                        new Guid("{3C58D432-147C-491A-A34A-4A88B2CCBCB5}"),
                        new Guid("{2900D318-9207-4241-9CD0-A0B6D6DBC75F}"),
                        new Guid("{EF783D94-0418-4ABA-B653-6DB2A10E4B92}")
                    }.Contains(familyState))
                            item.WidDivWom = (item.WidDivWom ?? 0) + 1;
                    }
                    var bd2 = (DateTime?)person["Date_of_Birth"] ?? DateTime.MinValue;
                    var age = fd.Year - (bd2.Month < fd.Month ? bd2.Year - 1 : bd2.Year);
                    if (memCount == 0 && age >= 65)
                        item.OldAndSingle = (item.OldAndSingle ?? 0) + 1;
                    var employment = (Guid?)appState["employment"] ?? Guid.Empty;
                    if (employment == unemployed)
                        item.Unemploy = (item.Unemploy ?? 0) + 1;
                }
                var count = 0;
                foreach (var item in items)
                {
                    var rItem = new Appointed();
                    if (item.DistrictId != Guid.Empty)
                        rItem.district = context.Documents.LoadById(item.DistrictId)["Name"].ToString();
                    if (item.DjamoatId != Guid.Empty)
                        rItem.djamoat = context.Documents.LoadById(item.DjamoatId)["Name"].ToString();
                    rItem.NumberOfApplications = item.Count is null ? 0 : (int)item.Count;
                    rItem.NumberFM = item.FemAll is null ? 0 : (int)item.FemAll;
                    rItem.ChildrenUn16 = item.Child16 is null ? 0 : (int)item.Child16;
                    rItem.OverAge = item.Child18 is null ? 0 : (int)item.Child18;
                    rItem.ChildrenDisability = item.ChildDis1Gr is null ? 0 : (int)item.ChildDis1Gr;
                    rItem.Over_18_Years = item.Child18ST is null ? 0 : (int)item.Child18ST;
                    rItem.ChildrenDisability = item.ChildDis23Gr is null ? 0 : (int)item.ChildDis23Gr;
                    rItem.WomenHeadsFamily = item.WidDivWom is null ? 0 : (int)item.WidDivWom;
                    rItem.LonelyElderly = item.OldAndSingle is null ? 0 : (int)item.OldAndSingle;
                    rItem.Unemployed = item.Unemploy is null ? 0 : (int)item.Unemploy;
                    rItem.quantitativeData = generalReportItem.quantitativeData;
                    generalReportItem.appointedList.Add(rItem);
                    count++;
                }
                if (distrId != Guid.Empty)
                    generalReportItem.quantitativeData.district = context.Documents.LoadById(distrId)["Name"].ToString();
                generalReportItem.quantitativeData.QuantitativeAppCount = count;
                return generalReportItem;
            }

            private static ReportItem GetItem(List<ReportItem> items, Guid districtId, Guid djamoatId)
            {
                var item = items.FirstOrDefault(i => i.DistrictId == districtId && i.DjamoatId == djamoatId);
                if (item != null)
                    return item;
                var newItem = new ReportItem { DistrictId = districtId, DjamoatId = djamoatId };
                items.Add(newItem);
                return newItem;
            }
            private static double GetBall(WorkflowContext context, DateTime fd, DateTime ld)
            {
                var ballDefId = new Guid("{E6133A73-23B7-4CCF-B144-5EF6A94308FA}");
                var qb = new QueryBuilder(ballDefId);
                qb.Where("From").Le(fd).And("To").Ge(ld);
                var query = context.CreateSqlQuery(qb.Def);
                query.AddAttribute("Value");
                using (var reader = context.CreateSqlReader(query))
                {
                    if (reader.Read())
                        return reader.IsDbNull(0) ? 0.0 : reader.GetDouble(0);
                }
                return 0.0;
            }
            private static readonly Guid reportDefId = new Guid("{72C1A0EB-4D25-4689-823E-1B4415584CE8}"); //Количественные данные по численности охваченных программой АСП
            private static readonly Guid reportItemDefId = new Guid("{C35CC3D7-E59F-46AA-BB6E-A6FB6B8029CF}"); //Количество присужденных заявлений
            public static readonly Guid AppDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}"); //Заявление на АСП
            public static readonly Guid AppStateDefId = new Guid("{547BBA55-2281-4388-A1FC-EE890168AC2D}");
            public static readonly Guid womanSexId = new Guid("{56E07640-5B5B-47FA-832D-A6639F36EB71}");
            public static readonly Guid notGroup = new Guid("{C3EB23D0-9806-4C9B-B087-5987B763086D}"); //нет группа инвалидности
            public static readonly Guid FirstGroup = new Guid("{CC195B10-995A-44DB-A852-F519E0AD1BED}"); //1 группа
            public static readonly Guid SecondGroup = new Guid("{5FB9A008-071E-4FD5-B027-E197B4C833E0}"); //2 группа
            public static readonly Guid ThirdGroup = new Guid("{715D3EF3-A08B-47C6-8C5E-2555A5E1253A}"); //3 группа

            public static readonly Guid unemployed = new Guid("{0B8F9451-A7E6-4BE5-8A9D-5F4B87B9BEC4}");
            public static readonly Guid AssignedStateId = new Guid("{ACB44CC8-BF44-44F4-8056-723CED22536C}");
            public static readonly Guid OnPaymentStateId = new Guid("{78C294B5-B6EA-4075-9EEF-52073A6A2511}");
            public static readonly Guid ReadyAppointStateId = new Guid("{0AE798A6-5471-4E2C-999E-7371799F6AD0}"); //Зарегистрирован

            public class ReportItem
            {
                public Guid DistrictId;
                public Guid DjamoatId;
                public int? Count;
                public int? FemAll;
                public int? Child16;
                public int? Child18;
                public int? ChildDis1Gr;
                public int? Child18ST;
                public int? ChildDis23Gr;
                public int? WidDivWom;
                public int? OldAndSingle;
                public int? Unemploy;
            }

            public class GeneralReportItem
            {
                public List<Appointed> appointedList { get; set; }
                public QuantitativeData quantitativeData { get; set; }
                public GeneralReportItem()
                {
                    appointedList = new List<Appointed>();
                    quantitativeData = new QuantitativeData();
                }
            }

        }

        public static class Report_105b
        {
            public static GeneralReportItem Execute(DateTime fd, DateTime ld, Guid districtId, Guid? djamoatId)
            {
                var reportItems = new List<Refuse>();
                var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));
                var generalReportItem = new GeneralReportItem();
                var docRepo = context.Documents;
                var distrId = districtId;
                //var djamId = djamoatId;
                var djamId = (Guid?)djamoatId ?? Guid.Empty;

                if (djamId != Guid.Empty)
                {
                    distrId = (Guid?)docRepo.LoadById(djamId)["District"] ?? Guid.Empty;
                }

                if (fd == DateTime.MinValue || ld == DateTime.MaxValue || ld <= fd)
                    throw new ApplicationException("Ошибка в периоде!");

                var qb = new QueryBuilder(AppDefId, context.UserId);
                qb.Where("Date").Ge(fd).And("Date").Le(ld).And("&State").In(new object[] { ReadyAppointStateId, RefusedStateId })
                .And("Application_State").Include("BALL").Gt(GetBall(context, fd, ld)).End();

                var table = new DataTable();
                using (var query = SqlQueryBuilder.Build(context.DataContext, qb.Def))
                {
                    var appStateSource = query.JoinSource(query.Source, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");
                    if (djamId != Guid.Empty)
                        query.AndCondition(appStateSource, "DjamoatId", ConditionOperation.Equal, djamId);
                    else if (distrId != Guid.Empty)
                        query.AndCondition(appStateSource, "DistrictId", ConditionOperation.Equal, distrId);

                    query.AddAttribute(query.Source, "&Id");
                    query.AddAttribute(appStateSource, "&Id");

                    using (var reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                }
                var items = new List<ReportItem>();
                foreach (DataRow row in table.Rows)
                {
                    var app = docRepo.LoadById((Guid)row[0]);
                    var appState = docRepo.LoadById((Guid)row[1]);
                    var district_Id = (Guid?)appState["DistrictId"] ?? Guid.Empty;
                    var djamoat_Id = (Guid?)appState["DjamoatId"] ?? Guid.Empty;

                    var item = GetItem(items, districtId, djamId);
                    item.Count = (item.Count ?? 0) + 1;

                    int memCount = 0;
                    int c;
                    foreach (var memId in docRepo.DocAttrList(out c, appState, "Family_Member", 0, 0))
                    {
                        memCount++;
                        item.FemAll = (item.FemAll ?? 0) + 1;
                        var mem = docRepo.LoadById(memId);
                        var mPerson = docRepo.LoadById((Guid)mem["Person"]);
                        var bd = (DateTime?)mPerson["Date_of_Birth"] ?? DateTime.MinValue;
                        if (bd != DateTime.MinValue)
                        {
                            var mAge = fd.Year - (bd.Month < fd.Month ? bd.Year - 1 : bd.Year);
                            if (mAge <= 16)
                                item.Child16 = (item.Child16 ?? 0) + 1;
                            var hasDis = (Guid?)mem["DisablilityGroupe"] ?? Guid.Empty;//(bool?)mem["HasDisability"] ?? false;

                            if (hasDis == FirstGroup)
                            {
                                if (mAge >= 18)
                                    item.Child18 = (item.Child18 ?? 0) + 1;
                                else
                                    item.ChildDis1Gr = (item.ChildDis1Gr ?? 0) + 1;
                            }
                            if (hasDis == SecondGroup || hasDis == ThirdGroup)
                            {
                                if (mAge >= 18)
                                    item.Child18ST = (item.Child18ST ?? 0) + 1;
                                else
                                    item.ChildDis23Gr = (item.ChildDis23Gr ?? 0) + 1;
                            }
                        }
                    }

                    var person = docRepo.LoadById((Guid)app["Person"]);
                    var sex = (Guid?)person["Sex"] ?? Guid.Empty;

                    if (sex == womanSexId)
                    {
                        var familyState = (Guid?)person["FamilyState"] ?? Guid.Empty;
                        if (new List<Guid>
                    {
                        new Guid("{3C58D432-147C-491A-A34A-4A88B2CCBCB5}"),
                        new Guid("{2900D318-9207-4241-9CD0-A0B6D6DBC75F}"),
                        new Guid("{EF783D94-0418-4ABA-B653-6DB2A10E4B92}")
                    }.Contains(familyState))
                            item.WidDivWom = (item.WidDivWom ?? 0) + 1;
                    }

                    var bd2 = (DateTime?)person["Date_of_Birth"] ?? DateTime.MinValue;
                    var age = fd.Year - (bd2.Month < fd.Month ? bd2.Year - 1 : bd2.Year);
                    if (memCount == 0 && age >= 65)
                        item.OldAndSingle = (item.OldAndSingle ?? 0) + 1;
                    var employment = (Guid?)appState["employment"] ?? Guid.Empty;
                    if (employment == unemployed)
                        item.Unemploy = (item.Unemploy ?? 0) + 1;
                }
                var count = 0;
                foreach (var item in items)
                {
                    var rItem = new Refuse();
                    if (item.DistrictId != Guid.Empty)
                        rItem.district = context.Documents.LoadById(item.DistrictId)["Name"].ToString();
                    if (item.DjamoatId != Guid.Empty)
                        rItem.djamoat = context.Documents.LoadById(item.DjamoatId)["Name"].ToString();
                    rItem.NumberCases = item.Count is null ? 0 : (int)item.Count;
                    rItem.FamilyMembers = item.FemAll is null ? 0 : (int)item.FemAll;
                    rItem.ChildrenUnder_16 = item.Child16 is null ? 0 : (int)item.Child16;
                    rItem.DisabledGroup_1 = item.Child18 is null ? 0 : (int)item.Child18;
                    rItem.ChildrenDisabilities = item.ChildDis1Gr is null ? 0 : (int)item.ChildDis1Gr;
                    rItem.Disabled = item.Child18ST is null ? 0 : (int)item.Child18ST;
                    rItem.DisabledChild = item.ChildDis23Gr is null ? 0 : (int)item.ChildDis23Gr;
                    rItem.Women_Heads_Family = item.WidDivWom is null ? 0 : (int)item.WidDivWom;
                    rItem.LonelyElderly = item.OldAndSingle is null ? 0 : (int)item.OldAndSingle;
                    rItem.Unemployed = item.Unemploy is null ? 0 : (int)item.Unemploy;
                    rItem.quantitativeDataRefusal = generalReportItem.quantitativeDataRefusal;
                    generalReportItem.refuseList.Add(rItem);
                    count++;
                }
                if (distrId != Guid.Empty)
                {
                    generalReportItem.quantitativeDataRefusal.district = context.Documents.LoadById(distrId)["Name"].ToString();
                }
                if (djamId != Guid.Empty)
                {
                    generalReportItem.quantitativeDataRefusal.djamoat = context.Documents.LoadById(djamId)["Name"].ToString();
                }
                generalReportItem.quantitativeDataRefusal.QuantitativeAppCountB = count;
                return generalReportItem;
            }

            private static ReportItem GetItem(List<ReportItem> items, Guid districtId, Guid djamoatId)
            {
                var item = items.FirstOrDefault(i => i.DistrictId == districtId && i.DjamoatId == djamoatId);
                if (item != null)
                    return item;
                var newItem = new ReportItem { DistrictId = districtId, DjamoatId = djamoatId };
                items.Add(newItem);
                return newItem;
            }
            private static double GetBall(WorkflowContext context, DateTime fd, DateTime ld)
            {
                var ballDefId = new Guid("{E6133A73-23B7-4CCF-B144-5EF6A94308FA}");
                var qb = new QueryBuilder(ballDefId);
                qb.Where("From").Le(fd).And("To").Ge(ld);
                var query = context.CreateSqlQuery(qb.Def);
                query.AddAttribute("Value");
                using (var reader = context.CreateSqlReader(query))
                {
                    if (reader.Read())
                        return reader.IsDbNull(0) ? 0.0 : reader.GetDouble(0);
                }
                return 0.0;
            }

            private static readonly Guid reportDefId = new Guid("{7E6F9B8F-3E82-4868-9089-60C2419ABBA4}"); //Количественные данные по численности охваченных программой АСП
            private static readonly Guid reportItemDefId = new Guid("{E39E48F3-11B1-4172-B51B-3BB9824C0153}"); //Количество присужденных заявлений
            public static readonly Guid AppDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}"); //Заявление на АСП 
            public static readonly Guid AppStateDefId = new Guid("{547BBA55-2281-4388-A1FC-EE890168AC2D}");
            public static readonly Guid womanSexId = new Guid("{56E07640-5B5B-47FA-832D-A6639F36EB71}");
            public static readonly Guid notGroup = new Guid("{C3EB23D0-9806-4C9B-B087-5987B763086D}"); //нет группа инвалидности
            public static readonly Guid FirstGroup = new Guid("{CC195B10-995A-44DB-A852-F519E0AD1BED}"); //1 группа
            public static readonly Guid SecondGroup = new Guid("{5FB9A008-071E-4FD5-B027-E197B4C833E0}"); //2 группа
            public static readonly Guid ThirdGroup = new Guid("{715D3EF3-A08B-47C6-8C5E-2555A5E1253A}"); //3 группа
            public static readonly Guid unemployed = new Guid("{0B8F9451-A7E6-4BE5-8A9D-5F4B87B9BEC4}");
            public static readonly Guid ReadyAppointStateId = new Guid("{0AE798A6-5471-4E2C-999E-7371799F6AD0}"); //Зарегистрирован
            public static readonly Guid RefusedStateId = new Guid("{5D8FF804-E287-41D5-8594-35A333F3CB49}"); //Отказано     

            public class ReportItem
            {
                public Guid DistrictId;
                public Guid DjamoatId;
                public int? Count;
                public int? FemAll;
                public int? Child16;
                public int? Child18;
                public int? ChildDis1Gr;
                public int? Child18ST;
                public int? ChildDis23Gr;
                public int? WidDivWom;
                public int? OldAndSingle;
                public int? Unemploy;
            }

            public class GeneralReportItem
            {
                public List<Refuse> refuseList { get; set; }
                public QuantitativeDataRefusal quantitativeDataRefusal { get; set; }
                public GeneralReportItem()
                {
                    refuseList = new List<Refuse>();
                    quantitativeDataRefusal = new QuantitativeDataRefusal();
                }
            }
        }

        public static class Report_106
        {
            public static GeneralReportItem Execute(DateTime fd, DateTime ld, Guid districtId, Guid? djamoatId)
            {
                var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));
                var distrId = (Guid?)districtId ?? Guid.Empty;
                var djamId = (Guid?)djamoatId ?? Guid.Empty;
                return Build(context, fd, ld, distrId, djamId);

            }
            /**************************************************************************************************************************/
            private static readonly Guid reportDefId = new Guid("{7A9070C8-130E-43B4-9EDC-E2C027D2209C}");      //Отчет  
            private static readonly Guid reportItemDefId = new Guid("{547E3398-4008-49FA-996B-976194A2A408}");  //строки  
            public static readonly Guid BankAccountDefId = new Guid("{BE6D5C1F-48A6-483B-980A-14CEFF781FD4}");
            public static readonly Guid BankRegistryDefId = new Guid("{ADF1D21A-5FCE-4F42-8889-D0714DDF7967}");
            public static readonly Guid BankPaymentRegistryDefId = new Guid("{B3BB3306-C3B4-4F67-98BF-B015DEEDEFFF}"); //
            public static readonly Guid BankTernoverDefId = new Guid("{982C14E1-3F9C-4559-8835-314C612AB021}");
            public static readonly Guid BankTernoverReportDefId = new Guid("{BF01AAF9-4838-42C9-8F47-0171DCCD9C3D}");
            public static readonly Guid BankPayRegDisNoDefId = new Guid("{DB434DEC-259F-4563-9213-301D9E38753D}"); //Реестр на оплату. Район. Номер выплаты
            public static readonly Guid AppDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}"); //Заявление на АСП
            public static readonly Guid PersonDefId = new Guid("{6052978A-1ECB-4F96-A16B-93548936AFC0}"); //Гражданин
            private static readonly Guid paymentDefId = new Guid("{68667FBB-C149-4FB3-93AD-1BBCE3936B6E}");
            private static readonly Guid assignmentDefId = new Guid("{51935CC6-CC48-4DAC-8853-DA8F57C057E8}");
            public static readonly Guid AppStateDefId = new Guid("{547BBA55-2281-4388-A1FC-EE890168AC2D}");
            public static readonly Guid DistrictId = new Guid("{4D029337-C025-442E-8E93-AFD1852073AC}");
            public static readonly Guid DjamoatId = new Guid("{967D525D-9B76-44BE-93FA-BD4639EA515A}");
            public static readonly Guid VillageId = new Guid("{B70BAD68-7532-471F-A705-364FD5F1BA9E}");

            private static readonly Guid BankPayRegDisNoStateId = new Guid("{9BCE67C9-DD5D-42BC-9D07-E194CD3A804C}");  //На оплате
                                                                                                                       /**************************************************************************************************************************/
            public static GeneralReportItem Build(WorkflowContext context, DateTime fd, DateTime ld, Guid distrId, Guid djamId)
            {
                var docRepo = context.Documents;
                //  var report = context.CurrentDocument;
                var generalReportItem = new GeneralReportItem();
                /****************************************************************************************************************/
                if (djamId != Guid.Empty)
                {
                    distrId = (Guid?)docRepo.LoadById(djamId)["District"] ?? Guid.Empty;
                }
                if (fd == DateTime.MinValue || ld == DateTime.MaxValue || ld <= fd)
                    throw new ApplicationException("Ошибка в периоде!");

                var qb = new QueryBuilder(paymentDefId);
                using (var query = context.CreateSqlQuery(qb.Def))
                {
                    var bankSrc = query.JoinSource(query.Source, BankAccountDefId, SqlSourceJoinType.Inner, "BankAccount");
                    var bankRegSrc = query.JoinSource(query.Source, BankRegistryDefId, SqlSourceJoinType.Inner, "Registry");
                    var bankPayRegSrc = query.JoinSource(bankRegSrc, BankPaymentRegistryDefId, SqlSourceJoinType.Inner, "BankPaymentRegistry");
                    var bankRegStateSrc = query.JoinSource(query.Source, BankPayRegDisNoDefId, SqlSourceJoinType.Inner, "Registry_DistrictNo");
                    var appSrc = query.JoinSource(bankSrc, AppDefId, SqlSourceJoinType.Inner, "Application");
                    var personSrc = query.JoinSource(appSrc, PersonDefId, SqlSourceJoinType.Inner, "Person");

                    var appStateSrc = query.JoinSource(appSrc, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");

                    query.AndCondition(bankPayRegSrc, "Date", ConditionOperation.GreatEqual, fd);
                    query.AndCondition(bankPayRegSrc, "Date", ConditionOperation.LessThen, ld.AddDays(1));
                    query.AndCondition(bankRegStateSrc, "&State", ConditionOperation.Equal, BankPayRegDisNoStateId);
                    if (djamId != Guid.Empty)
                        query.AndCondition(appStateSrc, "DjamoatId", ConditionOperation.Equal, djamId);
                    else if (distrId != Guid.Empty)
                        query.AndCondition(appStateSrc, "DistrictId", ConditionOperation.Equal, distrId);

                    query.AddAttribute(bankSrc, "&Id");
                    query.AddAttribute(bankSrc, "Account_No");
                    query.AddAttribute(appSrc, "No");
                    query.AddAttribute(personSrc, "Last_Name");
                    query.AddAttribute(personSrc, "First_Name");
                    query.AddAttribute(personSrc, "Middle_Name");
                    query.AddAttribute(personSrc, "PassportSeries");
                    query.AddAttribute(personSrc, "PassportNo");
                    query.AddAttribute(appStateSrc, "DjamoatId");
                    query.AddAttribute(appStateSrc, "DistrictId");
                    query.AddAttribute(appStateSrc, "VillageId");
                    query.AddAttribute(appStateSrc, "street");
                    query.AddAttribute(appStateSrc, "House");
                    query.AddAttribute(appStateSrc, "flat");
                    var table = new DataTable();
                    using (var reader = context.CreateSqlReader(query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    var count = 0;
                    foreach (DataRow row in table.Rows)
                    {
                        var bankAccountId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                        var Account_No = row[1] is DBNull ? "-" : (string)row[1];
                        var No = row[2] is DBNull ? "-" : (string)row[2];
                        var Last_Name = row[3] is DBNull ? "-" : (string)row[3];
                        var First_Name = row[4] is DBNull ? "-" : (string)row[4];
                        var Middle_Name = row[5] is DBNull ? "-" : (string)row[5];
                        var PassportSeries = row[6] is DBNull ? "-" : (string)row[6];
                        var PassportNo = row[7] is DBNull ? "-" : (string)row[7];
                        var DjamoatId = row[8] is DBNull ? Guid.Empty : (Guid)row[8];
                        var DistrictId = row[9] is DBNull ? Guid.Empty : (Guid)row[9];
                        var VillageId = row[10] is DBNull ? Guid.Empty : (Guid)row[10];
                        var Street = row[11] is DBNull ? "-" : (string)row[11];
                        var House = row[12] is DBNull ? "-" : (string)row[12];
                        var Flat = row[13] is DBNull ? "-" : (string)row[13];

                        if (bankAccountId != Guid.Empty)
                        {
                            var qb1 = new QueryBuilder(BankTernoverDefId);
                            qb1.Where("BankAccount").Eq(bankAccountId).And("Report").Include("PeriodFrom").Le(ld).And("PeriodTo").Ge(fd).End();
                            using (var query1 = context.CreateSqlQuery(qb1.Def))
                            {
                                query1.AddAttribute("BalanceBegin");
                                query1.AddAttribute("Transferred");
                                query1.AddAttribute("Paid");

                                var balanceBegin = 0m;
                                var transferred = 0m;
                                var paid = 0m;
                                using (var reader = context.CreateSqlReader(query1))
                                {
                                    while (reader.Read())
                                    {
                                        balanceBegin = reader.IsDbNull(0) ? 0 : reader.GetDecimal(0);
                                        transferred = reader.IsDbNull(1) ? 0 : reader.GetDecimal(1);
                                        paid = reader.IsDbNull(2) ? 0 : reader.GetDecimal(2);
                                    }
                                }

                                var reportItem = new ReportItem();
                                reportItem.Appointed = balanceBegin + transferred;
                                reportItem.Paid = paid;
                                reportItem.AccountNo = Account_No;
                                reportItem.ApplicationNo = No;
                                reportItem.LastName = Last_Name;
                                reportItem.FirstName = First_Name;
                                reportItem.MiddleName = Middle_Name;
                                reportItem.PassportSeries = PassportSeries;
                                reportItem.PassportNo = PassportNo;
                                reportItem.DistrictName = context.Documents.LoadById(DistrictId)["Name"].ToString();
                                reportItem.DjamoatName = context.Documents.LoadById(DjamoatId)["Name"].ToString();
                                reportItem.VillageName = context.Documents.LoadById(VillageId)["Name"].ToString();
                                reportItem.Street = Street;
                                reportItem.House = House;
                                reportItem.Flat = Flat;
                                generalReportItem.reportItems.Add(reportItem);
                                count++;
                            }
                        }
                    }
                    if (distrId != Guid.Empty)
                    {
                        generalReportItem.districtName = context.Documents.LoadById(distrId)["Name"].ToString();
                    }
                    if (djamId != Guid.Empty)
                    {
                        generalReportItem.djamoatName = context.Documents.LoadById(djamId)["Name"].ToString();
                    }
                    generalReportItem.assignedPaidCount = count;
                    return generalReportItem;
                }
            }

            [DataContract]
            public class GeneralReportItem
            {
                [DataMember]
                public List<ReportItem> reportItems { get; set; }
                [DataMember]
                public string districtName { get; set; }
                [DataMember]
                public string djamoatName { get; set; }
                [DataMember]
                public int assignedPaidCount { get; set; }
                public GeneralReportItem()
                {
                    reportItems = new List<ReportItem>();
                }
            }

            [DataContract]
            public class ReportItem
            {
                [DataMember]
                public string AccountNo { get; set; }
                [DataMember]
                public string ApplicationNo { get; set; }
                [DataMember]
                public string LastName { get; set; }
                [DataMember]
                public string FirstName { get; set; }
                [DataMember]
                public string MiddleName { get; set; }
                [DataMember]
                public string PassportSeries { get; set; }
                [DataMember]
                public string PassportNo { get; set; }
                [DataMember]
                public string DistrictName { get; set; }
                [DataMember]
                public string DjamoatName { get; set; }
                [DataMember]
                public string VillageName { get; set; }
                [DataMember]
                public string Street { get; set; }
                [DataMember]
                public string House { get; set; }
                [DataMember]
                public string Flat { get; set; }
                [DataMember]
                public decimal Appointed { get; set; }
                [DataMember]
                public decimal Paid { get; set; }

            }

        }

        public static class Report_111
        {
            public static ListFamiliesMain Execute(DateTime fd, DateTime ld, Guid districtId, Guid? djamoatId)
            {
                var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));
                var listFamiliesMain = new ListFamiliesMain();
                var docRepo = context.Documents;

                var distrId = (Guid?)districtId ?? Guid.Empty;
                var djamId = (Guid?)djamoatId ?? Guid.Empty;
                if (djamId != Guid.Empty)
                {
                    distrId = (Guid?)docRepo.LoadById(djamId)["District"] ?? Guid.Empty;
                }
                if (fd == DateTime.MinValue || ld == DateTime.MaxValue || ld <= fd)
                    throw new ApplicationException("Ошибка в периоде!");

                var items = new List<ReportItem>();
                var qb = new QueryBuilder(AppDefId);

                using (var query = context.CreateSqlQuery(qb.Def))
                {
                    var appStateSource = query.JoinSource(query.Source, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");
                    query.AndCondition(query.Source, "Date", ConditionOperation.GreatEqual, fd);
                    query.AndCondition(query.Source, "Date", ConditionOperation.LessThen, ld.AddDays(1));
                    query.AndCondition(query.Source, "&State", ConditionOperation.NotEqual, registrStateId);
                    query.AndCondition(query.Source, "&State", ConditionOperation.NotEqual, refusedStateId);
                    if (djamId != Guid.Empty)
                        query.AndCondition(appStateSource, "DjamoatId", ConditionOperation.Equal, djamId);
                    else if (distrId != Guid.Empty)
                        query.AndCondition(appStateSource, "DistrictId", ConditionOperation.Equal, distrId);

                    query.AddAttribute(appStateSource, "DistrictId");
                    query.AddAttribute(appStateSource, "DjamoatId");
                    query.AddAttribute(query.Source, "&State");
                    query.AddAttribute(appStateSource, "All");
                    query.AddAttribute(query.Source, "&Id");

                    using (var reader = context.CreateSqlReader(query))
                    {
                        while (reader.Read())
                        {
                            var district_Id = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                            var djamoat_Id = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                            var stateId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                            var allFamilyCount = reader.IsDbNull(3) ? 0 : reader.GetInt32(3);
                            if (djamoat_Id == Guid.Empty && district_Id == Guid.Empty && stateId == Guid.Empty) continue;
                            var item = GetItem(items, district_Id, djamoat_Id, stateId == assignedStateId || stateId == paydStateId);
                            item.Count = (item.Count ?? 0) + 1;
                            item.MemCount = (item.MemCount ?? 0) + (allFamilyCount);
                        }
                    }
                }
                var count = 0;
                foreach (var djId in items.GroupBy(i => i.DjamoatId).Select(x => x.Key).Distinct())
                {
                    var list = items.Where(x => x.DjamoatId == djId);
                    var rItem = new Family();

                    if (djId != Guid.Empty)
                    {
                        rItem.djamoat = context.Documents.LoadById(djId)["Name"].ToString();
                    }
                    var district_Id = list.First().DistrictId;
                    if (district_Id != Guid.Empty)
                    {
                        rItem.district = context.Documents.LoadById(districtId)["Name"].ToString();
                    }
                    rItem.NumberApplicants = (int)list.Sum(x => x.Count);
                    rItem.NumberDesignatedBenefits = (int)list.Where(x => x.IsAssign).Sum(x => x.Count);
                    rItem.NumberFamilyMembers = (int)list.Sum(x => x.MemCount);
                    count++;
                    listFamiliesMain.listFamilies.Add(rItem);
                }
                if (distrId != Guid.Empty)
                {
                    listFamiliesMain.district = context.Documents.LoadById(districtId)["Name"].ToString();
                    listFamiliesMain.FamCount = count;
                }
                return listFamiliesMain;
            }
            private static ReportItem GetItem(List<ReportItem> items, Guid districtId, Guid djamoatId, bool isAssign)
            {
                var item = items.FirstOrDefault(x => x.DistrictId == districtId && x.DjamoatId == djamoatId && (x.IsAssign ? isAssign : true));
                if (item != null)
                    return item;
                var newItem = new ReportItem
                {
                    DistrictId = districtId,
                    DjamoatId = djamoatId,
                    IsAssign = isAssign
                };
                items.Add(newItem);
                return newItem;
            }
            private static readonly Guid reportDefId = new Guid("{CA6C194C-A14A-47DD-8D70-9A5356DFF68C}");
            private static readonly Guid reportItemDefId = new Guid("{32C6BCB1-60C0-474C-AC36-553EF3788D47}");
            public static readonly Guid AppDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}"); //Заявление на АСП
            public static readonly Guid AppStateDefId = new Guid("{547BBA55-2281-4388-A1FC-EE890168AC2D}");

            public static readonly Guid FamilyMemberDefId = new Guid("{959762D8-FEB6-4247-986D-7ECE63EED1AD}"); //Член домохозяйства
            public static readonly Guid PersonDefId = new Guid("{6052978A-1ECB-4F96-A16B-93548936AFC0}"); //Гражданин

            private static readonly Guid registrStateId = new Guid("{F062290E-42E3-4E24-8547-9FA7606547D7}"); //На регистрации 
            private static readonly Guid refusedStateId = new Guid("{48BC65B8-0C18-4DEA-9948-DDCE279E3F0E}"); //отказано по ошибке 

            private static readonly Guid assignedStateId = new Guid("{ACB44CC8-BF44-44F4-8056-723CED22536C}"); //Назначено
            private static readonly Guid paydStateId = new Guid("{78C294B5-B6EA-4075-9EEF-52073A6A2511}"); //на выплате


            public class ReportItem
            {
                public Guid DistrictId;
                public Guid DjamoatId;
                public bool IsAssign;
                public int? MemCount;
                public int? Count;
            }

            [DataContract]
            public class ListFamiliesMain
            {
                [DataMember]
                public DateTime DateFormation { get; set; }
                [DataMember]
                public string district { get; set; }
                [DataMember]
                public string djamoat { get; set; }
                [DataMember]
                public DateTime From { get; set; }
                [DataMember]
                public DateTime To { get; set; }
                [DataMember]
                public List<Family> listFamilies { get; set; }
                [DataMember]
                public int FamCount { get; set; }
                public ListFamiliesMain()
                {
                    listFamilies = new List<Family>();
                }
            }

            [DataContract]
            public class Family
            {
                [DataMember]
                public int NumberFamilyMembers { get; set; }
                [DataMember]
                public int NumberApplicants { get; set; }
                [DataMember]
                public string district { get; set; }
                [DataMember]
                public string djamoat { get; set; }
                [DataMember]
                public int NumberDesignatedBenefits { get; set; }

            }
        }

        public static class Report_112
        {

            public static GeneralReportItem Execute(DateTime fd, DateTime ld, Guid districtId, Guid? djamoatId)
            {
                var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));
                var docRepo = context.Documents;

                var distrId = (Guid?)districtId ?? Guid.Empty;
                var djamId = (Guid?)djamoatId ?? Guid.Empty;
                return Build(context, fd, ld, distrId, djamId);
            }
            /*************************************************************************************************/
            private static readonly Guid reportDefId = new Guid("{7C5B21D1-53C8-43AF-BCAF-07BF63F9019B}");
            private static readonly Guid reportItemDefId = new Guid("{BB50C5C5-0F05-4FFD-801D-B4D773B6C3CD}");
            public static readonly Guid BankAccountDefId = new Guid("{BE6D5C1F-48A6-483B-980A-14CEFF781FD4}");
            public static readonly Guid AppDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}");
            public static readonly Guid AppStateDefId = new Guid("{547BBA55-2281-4388-A1FC-EE890168AC2D}");
            public static readonly Guid PersonDefId = new Guid("{6052978A-1ECB-4F96-A16B-93548936AFC0}"); //Гражданин
            /****************************************************************************************************/
            public static GeneralReportItem Build(WorkflowContext context, DateTime fd, DateTime ld, Guid distrId, Guid djamId)
            {
                var docRepo = context.Documents;
                var generalReportItem = new GeneralReportItem();

                /*****************************************************************************************************/
                if (djamId != Guid.Empty)
                {
                    distrId = (Guid?)docRepo.LoadById(djamId)["District"] ?? Guid.Empty;
                }

                if (fd == DateTime.MinValue || ld == DateTime.MaxValue || ld <= fd)
                    throw new ApplicationException("Ошибка в периоде!");

                var qb = new QueryBuilder(BankAccountDefId);
                using (var query = context.CreateSqlQuery(qb.Def))
                {
                    var appSrc = query.JoinSource(query.Source, AppDefId, SqlSourceJoinType.Inner, "Application");
                    var personSrc = query.JoinSource(appSrc, PersonDefId, SqlSourceJoinType.Inner, "Person");
                    var appStateSrc = query.JoinSource(appSrc, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");

                    query.AndCondition(appSrc, "Date", ConditionOperation.GreatEqual, fd);
                    query.AndCondition(appSrc, "Date", ConditionOperation.LessThen, ld.AddDays(1));
                    if (djamId != Guid.Empty)
                        query.AndCondition(appStateSrc, "DjamoatId", ConditionOperation.Equal, djamId);
                    else if (distrId != Guid.Empty)
                        query.AndCondition(appStateSrc, "DistrictId", ConditionOperation.Equal, distrId);

                    query.AddAttribute(query.Source, "&Id");
                    query.AddAttribute(query.Source, "Account_No");
                    query.AddAttribute(appSrc, "No");
                    query.AddAttribute(personSrc, "Last_Name");
                    query.AddAttribute(personSrc, "First_Name");
                    query.AddAttribute(personSrc, "Middle_Name");
                    query.AddAttribute(personSrc, "PassportSeries");
                    query.AddAttribute(personSrc, "PassportNo");
                    query.AddAttribute(appStateSrc, "DjamoatId");
                    query.AddAttribute(appStateSrc, "DistrictId");
                    query.AddAttribute(appStateSrc, "VillageId");
                    query.AddAttribute(appStateSrc, "street");
                    query.AddAttribute(appStateSrc, "House");
                    query.AddAttribute(appStateSrc, "flat");


                    var table = new DataTable();

                    using (var reader = context.CreateSqlReader(query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    var count = 0;
                    foreach (DataRow row in table.Rows)
                    {
                        var bankAccountId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                        var Account_No = row[1] is DBNull ? "-" : (string)row[1];
                        var No = row[2] is DBNull ? "-" : (string)row[2];
                        var Last_Name = row[3] is DBNull ? "-" : (string)row[3];
                        var First_Name = row[4] is DBNull ? "-" : (string)row[4];
                        var Middle_Name = row[5] is DBNull ? "-" : (string)row[5];
                        var PassportSeries = row[6] is DBNull ? "-" : (string)row[6];
                        var PassportNo = row[7] is DBNull ? "-" : (string)row[7];
                        var DjamoatId = row[8] is DBNull ? Guid.Empty : (Guid)row[8];
                        var DistrictId = row[9] is DBNull ? Guid.Empty : (Guid)row[9];
                        var VillageId = row[10] is DBNull ? Guid.Empty : (Guid)row[10];
                        var Street = row[11] is DBNull ? "-" : (string)row[11];
                        var House = row[12] is DBNull ? "-" : (string)row[12];
                        var Flat = row[13] is DBNull ? "-" : (string)row[13];

                        if (bankAccountId != Guid.Empty)
                        {
                            var reportItem = new ReportItem();
                            reportItem.AccountNo = Account_No;
                            reportItem.ApplicationNo = No;
                            reportItem.LastName = Last_Name;
                            reportItem.FirstName = First_Name;
                            reportItem.MiddleName = Middle_Name;
                            reportItem.PassportSeries = PassportSeries;
                            reportItem.PassportNo = PassportNo;
                            reportItem.DistrictName = context.Documents.LoadById(DistrictId)["Name"].ToString();
                            reportItem.DjamoatName = context.Documents.LoadById(DjamoatId)["Name"].ToString();
                            reportItem.VillageName = context.Documents.LoadById(VillageId)["Name"].ToString();
                            reportItem.Street = Street;
                            reportItem.House = House;
                            reportItem.Flat = Flat;
                            count++;
                            generalReportItem.reportItems.Add(reportItem);
                        }
                    }
                    generalReportItem.DateFormation = DateTime.Today;
                    generalReportItem.From = fd;
                    generalReportItem.To = ld;
                    if (distrId != Guid.Empty)
                    {
                        generalReportItem.districtName = context.Documents.LoadById(distrId)["Name"].ToString();
                    }
                    if (djamId != Guid.Empty)
                    {
                        generalReportItem.djamoatName = context.Documents.LoadById(djamId)["Name"].ToString();
                    }
                    generalReportItem.RecipientsCount = count;
                    return generalReportItem;
                }
            }

            [DataContract]
            public class ReportItem
            {
                [DataMember]
                public string AccountNo { get; set; }
                [DataMember]
                public string ApplicationNo { get; set; }
                [DataMember]
                public string LastName { get; set; }
                [DataMember]
                public string FirstName { get; set; }
                [DataMember]
                public string MiddleName { get; set; }
                [DataMember]
                public string PassportSeries { get; set; }
                [DataMember]
                public string PassportNo { get; set; }
                [DataMember]
                public string DistrictName { get; set; }
                [DataMember]
                public string DjamoatName { get; set; }
                [DataMember]
                public string VillageName { get; set; }
                [DataMember]
                public string Street { get; set; }
                [DataMember]
                public string House { get; set; }
                [DataMember]
                public string Flat { get; set; }
            }

            [DataContract]
            public class GeneralReportItem
            {
                [DataMember]
                public List<ReportItem> reportItems { get; set; }
                [DataMember]
                public DateTime DateFormation { get; set; }
                [DataMember]
                public DateTime From { get; set; }
                [DataMember]
                public DateTime To { get; set; }
                [DataMember]
                public string districtName { get; set; }
                [DataMember]
                public string djamoatName { get; set; }
                [DataMember]
                public int RecipientsCount { get; set; }
                public GeneralReportItem()
                {
                    reportItems = new List<ReportItem>();
                }
            }
        }

        public static class Report_113
        {
            /***************************************************************************************************/
            public static GeneralReportItem Execute(DateTime fd, DateTime ld, Guid districtId, Guid? djamoatId)
            {
                var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));
                var docRepo = context.Documents;

                var distrId = (Guid?)districtId ?? Guid.Empty;
                var djamId = (Guid?)djamoatId ?? Guid.Empty;
                return Build(context, fd, ld, distrId, djamId);
            }
            /****************************************************************************************************/
            private static readonly Guid reportDefId = new Guid("{24403536-B764-417E-B8DA-BEC32884F5AC}");
            private static readonly Guid reportItemDefId = new Guid("{E7435677-210E-4FEB-8DAA-BCC24B50986F}");
            private static readonly Guid NoticeDefId = new Guid("{60CE6E64-14C4-4B76-8582-A2077C45400C}");
            public static readonly Guid BankAccountDefId = new Guid("{BE6D5C1F-48A6-483B-980A-14CEFF781FD4}");
            public static readonly Guid AppDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}");
            public static readonly Guid NoticeStateId = new Guid("{81D603D0-9A19-49B8-A1D6-16CC645450D2}");
            private static readonly Guid assignmentDefId = new Guid("{51935CC6-CC48-4DAC-8853-DA8F57C057E8}");
            public static readonly Guid AppStateDefId = new Guid("{547BBA55-2281-4388-A1FC-EE890168AC2D}");
            public static readonly Guid ReasonDefId = new Guid("{CF856814-BD83-48ED-9253-BA3134A3C4D3}");



            public static readonly Guid BankRegistryDefId = new Guid("{ADF1D21A-5FCE-4F42-8889-D0714DDF7967}");
            public static readonly Guid BankPaymentRegistryDefId = new Guid("{B3BB3306-C3B4-4F67-98BF-B015DEEDEFFF}"); //
            public static readonly Guid BankTernoverDefId = new Guid("{982C14E1-3F9C-4559-8835-314C612AB021}");
            public static readonly Guid BankTernoverReportDefId = new Guid("{BF01AAF9-4838-42C9-8F47-0171DCCD9C3D}");
            public static readonly Guid BankPayRegDisNoDefId = new Guid("{DB434DEC-259F-4563-9213-301D9E38753D}"); //Реестр на оплату. Район. Номер выплаты
            public static readonly Guid PersonDefId = new Guid("{6052978A-1ECB-4F96-A16B-93548936AFC0}"); //Гражданин
            private static readonly Guid paymentDefId = new Guid("{68667FBB-C149-4FB3-93AD-1BBCE3936B6E}");

            public static readonly Guid DistrictId = new Guid("{4D029337-C025-442E-8E93-AFD1852073AC}");
            public static readonly Guid DjamoatId = new Guid("{967D525D-9B76-44BE-93FA-BD4639EA515A}");
            public static readonly Guid VillageId = new Guid("{B70BAD68-7532-471F-A705-364FD5F1BA9E}");

            /****************************************************************************************************/
            public static GeneralReportItem Build(WorkflowContext context, DateTime fd, DateTime ld, Guid distrId, Guid djamId)
            {
                var docRepo = context.Documents;
                var generalReportItem = new GeneralReportItem();
                /*****************************************************************************************************/
                var qb = new QueryBuilder(NoticeDefId);
                using (var query = context.CreateSqlQuery(qb.Def))
                {
                    var bankSrc = query.JoinSource(query.Source, BankAccountDefId, SqlSourceJoinType.Inner, "Bank_Account");

                    var appSrc = query.JoinSource(bankSrc, AppDefId, SqlSourceJoinType.Inner, "Application");
                    var personSrc = query.JoinSource(appSrc, PersonDefId, SqlSourceJoinType.Inner, "Person");

                    var appStateSrc = query.JoinSource(appSrc, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");

                    query.AndCondition(query.Source, "&State", ConditionOperation.Equal, NoticeStateId);

                    query.AndCondition(query.Source, "Date", ConditionOperation.GreatEqual, fd);
                    query.AndCondition(query.Source, "Date", ConditionOperation.LessThen, ld.AddDays(1));
                    if (djamId != Guid.Empty)
                        query.AndCondition(appStateSrc, "DjamoatId", ConditionOperation.Equal, djamId);
                    else if (distrId != Guid.Empty)
                        query.AndCondition(appStateSrc, "DistrictId", ConditionOperation.Equal, distrId);

                    query.AddAttribute(bankSrc, "&Id");
                    query.AddAttribute(bankSrc, "Account_No");
                    query.AddAttribute(appSrc, "No");
                    query.AddAttribute(personSrc, "Last_Name");
                    query.AddAttribute(personSrc, "First_Name");
                    query.AddAttribute(personSrc, "Middle_Name");
                    query.AddAttribute(personSrc, "PassportSeries");
                    query.AddAttribute(personSrc, "PassportNo");
                    query.AddAttribute(appStateSrc, "DjamoatId");
                    query.AddAttribute(appStateSrc, "DistrictId");
                    query.AddAttribute(appStateSrc, "VillageId");
                    query.AddAttribute(appStateSrc, "street");
                    query.AddAttribute(appStateSrc, "House");
                    query.AddAttribute(appStateSrc, "flat");

                    query.AddAttribute(query.Source, "&Id");
                    query.AddAttribute(query.Source, "Reason");

                    var table = new DataTable();
                    using (var reader = context.CreateSqlReader(query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    var count = 0;
                    foreach (DataRow row in table.Rows)
                    {
                        var bankAccountId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                        var Account_No = row[1] is DBNull ? "-" : (string)row[1];
                        var No = row[2] is DBNull ? "-" : (string)row[2];
                        var Last_Name = row[3] is DBNull ? "-" : (string)row[3];
                        var First_Name = row[4] is DBNull ? "-" : (string)row[4];
                        var Middle_Name = row[5] is DBNull ? "-" : (string)row[5];
                        var PassportSeries = row[6] is DBNull ? "-" : (string)row[6];
                        var PassportNo = row[7] is DBNull ? "-" : (string)row[7];
                        var DjamoatId = row[8] is DBNull ? Guid.Empty : (Guid)row[8];
                        var DistrictId = row[9] is DBNull ? Guid.Empty : (Guid)row[9];
                        var VillageId = row[10] is DBNull ? Guid.Empty : (Guid)row[10];
                        var Street = row[11] is DBNull ? "-" : (string)row[11];
                        var House = row[12] is DBNull ? "-" : (string)row[12];
                        var Flat = row[13] is DBNull ? "-" : (string)row[13];
                        var noticeId = row[14] is DBNull ? Guid.Empty : (Guid)row[14];
                        var reason = row[15] is DBNull ? Guid.Empty : (Guid)row[15];

                        if (bankAccountId != Guid.Empty && noticeId != Guid.Empty)
                        {
                            var reportItem = new ReportItem();
                            reportItem.AccountNo = Account_No;
                            reportItem.ApplicationNo = No;
                            reportItem.LastName = Last_Name;
                            reportItem.FirstName = First_Name;
                            reportItem.MiddleName = Middle_Name;
                            reportItem.PassportSeries = PassportSeries;
                            reportItem.PassportNo = PassportNo;
                            reportItem.DistrictName = context.Documents.LoadById(DistrictId)["Name"].ToString();
                            reportItem.DjamoatName = context.Documents.LoadById(DjamoatId)["Name"].ToString();
                            reportItem.VillageName = context.Documents.LoadById(VillageId)["Name"].ToString();
                            reportItem.Street = Street;
                            reportItem.House = House;
                            reportItem.Flat = Flat;
                            reportItem.Reason = context.Enums.GetValue(noticeId).Value;
                            count++;
                            generalReportItem.reportItems.Add(reportItem);
                            if (distrId != Guid.Empty)
                            {
                                generalReportItem.districtName = context.Documents.LoadById(distrId)["Name"].ToString();
                            }
                        }
                    }

                    generalReportItem.DateFormation = DateTime.Today;
                    generalReportItem.From = fd;
                    generalReportItem.To = ld;
                    if (djamId != Guid.Empty)
                    {
                        generalReportItem.djamoatName = context.Documents.LoadById(djamId)["Name"].ToString();
                    }
                    generalReportItem.DiscontinuedPaymentsCount = count;

                    return generalReportItem;
                }
            }

            [DataContract]
            public class ReportItem
            {
                [DataMember]
                public string AccountNo { get; set; }
                [DataMember]
                public string ApplicationNo { get; set; }
                [DataMember]
                public string LastName { get; set; }
                [DataMember]
                public string FirstName { get; set; }
                [DataMember]
                public string MiddleName { get; set; }
                [DataMember]
                public string PassportSeries { get; set; }
                [DataMember]
                public string PassportNo { get; set; }
                [DataMember]
                public string DistrictName { get; set; }
                [DataMember]
                public string DjamoatName { get; set; }
                [DataMember]
                public string VillageName { get; set; }
                [DataMember]
                public string Street { get; set; }
                [DataMember]
                public string House { get; set; }
                [DataMember]
                public string Flat { get; set; }
                [DataMember]
                public string Reason { get; set; }


            }

            [DataContract]
            public class GeneralReportItem
            {
                [DataMember]
                public List<ReportItem> reportItems { get; set; }
                [DataMember]
                public DateTime DateFormation { get; set; }
                [DataMember]
                public DateTime From { get; set; }
                [DataMember]
                public DateTime To { get; set; }
                [DataMember]
                public string districtName { get; set; }
                [DataMember]
                public string djamoatName { get; set; }
                [DataMember]
                public int receiverCount { get; set; }
                [DataMember]
                public int DiscontinuedPaymentsCount { get; set; }
                public GeneralReportItem()
                {
                    reportItems = new List<ReportItem>();
                }
            }
        }

        public static class Report_114
        {
            /***************************************************************************************************/
            public static GeneralReportItem Execute(DateTime fd, DateTime ld, Guid districtId, Guid? djamoatId)
            {
                var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));
                var docRepo = context.Documents;

                var distrId = (Guid?)districtId ?? Guid.Empty;
                var djamId = (Guid?)djamoatId ?? Guid.Empty;
                return Build(context, fd, ld, distrId, djamId);
            }
            /****************************************************************************************************/
            private static readonly Guid reportDefId = new Guid("{749BA19E-5790-407A-A506-1A12C8CE85B0}");
            private static readonly Guid reportItemDefId = new Guid("{9B46D1C5-5DC8-4EDA-96C2-C2C6FEA8C852}");
            public static readonly Guid AppDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}");
            public static readonly Guid ComplaintDefId = new Guid("{9EF32F14-22D0-4854-A5FD-0B21868C0559}");
            public static readonly Guid For_official_useDefId = new Guid("{646419A9-3D66-4F46-A44D-D6FAD67538CE}");
            public static readonly Guid AppStateDefId = new Guid("{547BBA55-2281-4388-A1FC-EE890168AC2D}");
            public static readonly Guid AppealCompDefId = new Guid("{3072DACB-B9A1-49D8-925C-F4BE947C66B1}");
            public static readonly Guid PersonDefId = new Guid("{6052978A-1ECB-4F96-A16B-93548936AFC0}"); //Гражданин

            /****************************************************************************************************/
            public static GeneralReportItem Build(WorkflowContext context, DateTime fd, DateTime ld, Guid distrId, Guid djamId)
            {
                var docRepo = context.Documents;
                var generalReportItem = new GeneralReportItem();

                if (djamId != Guid.Empty)
                {
                    distrId = (Guid?)docRepo.LoadById(djamId)["District"] ?? Guid.Empty;
                }
                if (fd == DateTime.MinValue || ld == DateTime.MaxValue || ld <= fd)
                    throw new ApplicationException("Ошибка в периоде!");
                /*****************************************************************************************************/
                var sqlQueryBuilder = context.Get<ISqlQueryBuilder>();
                var query = sqlQueryBuilder.Build(ComplaintDefId);
                var userSrc = query.JoinSource(query.Source, For_official_useDefId, SqlSourceJoinType.LeftOuter, "For_official_use");
                var personSrc = query.JoinSource(query.Source, PersonDefId, SqlSourceJoinType.Inner, "Person");
                query.AndCondition("Date", ConditionOperation.GreatEqual, fd);
                query.AndCondition("Date", ConditionOperation.LessThen, ld.AddDays(1));
                query.AndCondition("&State", ConditionOperation.NotEqual, Guid.Empty);
                if (djamId != Guid.Empty)
                    query.AndCondition(query.Source, "DjamoatId", ConditionOperation.Equal, djamId);
                else if (distrId != Guid.Empty)
                    query.AndCondition(query.Source, "DistrictId", ConditionOperation.Equal, distrId);

                query.AddAttribute(query.Source, "Person");
                query.AddAttribute(personSrc, "Last_Name");
                query.AddAttribute(personSrc, "First_Name");
                query.AddAttribute(personSrc, "Middle_Name");
                query.AddAttribute(personSrc, "PassportSeries");
                query.AddAttribute(personSrc, "PassportNo");
                query.AddAttribute(query.Source, "Type_situtsii");
                query.AddAttribute(query.Source, "No");
                query.AddAttribute(userSrc, "fio1");
                query.AddAttribute(userSrc, "Date");
                query.AddAttribute(query.Source, "&Id");
                query.AddAttribute(query.Source, "DistrictId");

                var table = new DataTable();
                using (var reader = context.CreateSqlReader(query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }
                var count = 0;
                foreach (DataRow row in table.Rows)
                {
                    var personId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                    if (personId == Guid.Empty) continue;
                    var Last_Name = row[1] is DBNull ? "-" : (string)row[1];
                    var First_Name = row[2] is DBNull ? "-" : (string)row[2];
                    var Middle_Name = row[3] is DBNull ? "-" : (string)row[3];
                    var PassportSeries = row[4] is DBNull ? "-" : (string)row[4];
                    var PassportNo = row[5] is DBNull ? "-" : (string)row[5];
                    var TypeSitutsii = row[6] is DBNull ? Guid.Empty : (Guid)row[6];
                    string complaints = null;
                    if (TypeSitutsii != Guid.Empty)
                    {
                        var complaintsDefId = new Guid("{235707E5-6779-4685-B62B-2B9D512FFCDD}");
                        complaints = context.Enums.GetEnumValue(complaintsDefId, TypeSitutsii);
                    }
                    var no = row[7] is DBNull ? "" : row[7].ToString();
                    var userInfo = row[8] is DBNull ? "" : row[8].ToString();
                    var viewDate = row[9] is DBNull ? DateTime.MinValue : (DateTime)row[9];

                    var complaint = context.GetDynaDoc((Guid)row[10]);
                    var distId = row[11] is DBNull ? Guid.Empty : (Guid)row[12];
                    var complaintState = "";
                    if (complaint.State != null)
                        complaintState = complaint.State.Type.Name;

                    var reportItem = new ReportItem();
                    reportItem.LastName = Last_Name;
                    reportItem.FirstName = First_Name;
                    reportItem.MiddleName = Middle_Name;
                    reportItem.PassportSeries = PassportSeries;
                    reportItem.PassportNo = PassportNo;
                    reportItem.Grounds_Complaint_Or_Appeal = complaints;
                    reportItem.Number_Complaint_Or_Appeal = no;
                    reportItem.Considered = userInfo;
                    if (viewDate != DateTime.MinValue) reportItem.Date_Considered = viewDate;
                    reportItem.Status = complaintState;
                    count++;
                    generalReportItem.reportItems.Add(reportItem);
                }

                query = sqlQueryBuilder.Build(AppealCompDefId);
                var appSrc = query.JoinSource(query.Source, AppDefId, SqlSourceJoinType.Inner, "Application");
                var personSrc2 = query.JoinSource(appSrc, PersonDefId, SqlSourceJoinType.Inner, "Person");
                var appStateSrc = query.JoinSource(appSrc, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");
                query.AndCondition("Date", ConditionOperation.GreatEqual, fd);
                query.AndCondition("Date", ConditionOperation.LessThen, ld.AddDays(1));
                query.AndCondition("&State", ConditionOperation.NotEqual, Guid.Empty);
                if (djamId != Guid.Empty)
                    query.AndCondition(appStateSrc, "DjamoatId", ConditionOperation.Equal, djamId);
                else if (distrId != Guid.Empty)
                    query.AndCondition(appStateSrc, "DistrictId", ConditionOperation.Equal, distrId);

                query.AddAttribute(appSrc, "Person");
                query.AddAttribute(personSrc2, "Last_Name");
                query.AddAttribute(personSrc2, "First_Name");
                query.AddAttribute(personSrc2, "Middle_Name");
                query.AddAttribute(personSrc2, "PassportSeries");
                query.AddAttribute(personSrc2, "PassportNo");
                query.AddAttribute("No");
                query.AddAttribute(query.Source, "name");
                query.AddAttribute(query.Source, "Date");
                query.AddAttribute(query.Source, "&Id");
                query.AddAttribute(appStateSrc, "DistrictId");

                table = new DataTable();
                using (var reader = context.CreateSqlReader(query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }
                var count1 = 0;
                foreach (DataRow row in table.Rows)
                {
                    var personId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                    if (personId == Guid.Empty) continue;
                    var Last_Name = row[1] is DBNull ? "-" : (string)row[1];
                    var First_Name = row[2] is DBNull ? "-" : (string)row[2];
                    var Middle_Name = row[3] is DBNull ? "-" : (string)row[3];
                    var PassportSeries = row[4] is DBNull ? "-" : (string)row[4];
                    var PassportNo = row[5] is DBNull ? "-" : (string)row[5];
                    var no = row[6] is DBNull ? "" : row[6].ToString();
                    var userInfo = row[7] is DBNull ? "" : row[7].ToString();
                    var viewDate = row[8] is DBNull ? DateTime.MinValue : (DateTime)row[8];

                    var complaint = context.GetDynaDoc((Guid)row[4]);
                    var distId = row[5] is DBNull ? Guid.Empty : (Guid)row[5];

                    var complaintState = "";
                    if (complaint.State != null)
                        complaintState = complaint.State.Type.Name;

                    var reportItem = new ReportItem();
                    reportItem.LastName = Last_Name;
                    reportItem.FirstName = First_Name;
                    reportItem.MiddleName = Middle_Name;
                    reportItem.PassportSeries = PassportSeries;
                    reportItem.PassportNo = PassportNo;
                    reportItem.Number_Complaint_Or_Appeal = no;
                    reportItem.Considered = userInfo;

                    if (viewDate != DateTime.MinValue) reportItem.Date_Considered = viewDate;
                    reportItem.Status = complaintState;
                    count1++;
                    generalReportItem.reportItems.Add(reportItem);
                }
                generalReportItem.DateFormation = DateTime.Today;
                generalReportItem.From = fd;
                generalReportItem.To = ld;
                if (distrId != Guid.Empty)
                {
                    generalReportItem.districtName = context.Documents.LoadById(distrId)["Name"].ToString();
                }
                if (djamId != Guid.Empty)
                {
                    generalReportItem.djamoatName = context.Documents.LoadById(djamId)["Name"].ToString();
                }
                generalReportItem.CompAppCount = count + count1;
                return generalReportItem;
            }

            [DataContract]
            public class ReportItem
            {
                public string LastName { get; set; }
                [DataMember]
                public string FirstName { get; set; }
                [DataMember]
                public string MiddleName { get; set; }
                [DataMember]
                public string PassportSeries { get; set; }
                [DataMember]
                public string PassportNo { get; set; }
                [DataMember]
                public string Status { get; set; }
                [DataMember]
                public string Grounds_Complaint_Or_Appeal { get; set; }
                [DataMember]
                public DateTime Date_Considered { get; set; }
                [DataMember]
                public string Number_Complaint_Or_Appeal { get; set; }
                [DataMember]
                public string Considered { get; set; }

            }

            [DataContract]
            public class GeneralReportItem
            {
                [DataMember]
                public List<ReportItem> reportItems { get; set; }
                [DataMember]
                public DateTime DateFormation { get; set; }
                [DataMember]
                public DateTime From { get; set; }
                [DataMember]
                public DateTime To { get; set; }
                [DataMember]
                public string districtName { get; set; }
                [DataMember]
                public string djamoatName { get; set; }
                [DataMember]
                public int CompAppCount { get; set; }
                public GeneralReportItem()
                {
                    reportItems = new List<ReportItem>();
                }
            }
        }

        public static class Report_115
        {
            //Сведения о получателях АСП
            public static GeneralReportItem Execute(int year, int month)
            {
                var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));
                return Execute2(context, year, month);
            }
            public static GeneralReportItem Execute2(WorkflowContext context, int year, int month)
            {
                var generalReportItem = new GeneralReportItem();
                var docRepo = context.Documents;

                if (year < 2014 || year > 3000)
                    throw new ApplicationException("Ошибка в периоде!");
                if (month < 1 || month > 12)
                    throw new ApplicationException("Ошибка в значении месяца!");

                var fd = new DateTime(year, 1, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month)).AddDays(1);
                var fdLastYear = new DateTime(year - 1, 1, 1);
                var ldLastYear = new DateTime(year, 1, 1);


                /**********************************************************************************************/
                var qb = new QueryBuilder(AppDefId);
                qb.Where("Date").Ge(fdLastYear).And("Date").Lt(ld).And("&State").Neq(DeniedErrorTypeStateId);

                var sqlQueryBuilder = context.Get<ISqlQueryBuilder>();
                var query = sqlQueryBuilder.Build(qb.Def);
                var appStateSrc = query.JoinSource(query.Source, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");
                query.AddAttribute(query.Source, "&Id");
                query.AddAttribute(query.Source, "&State");
                query.AddAttribute(query.Source, "Date");
                query.AddAttribute(appStateSrc, "RegionId");
                query.AddAttribute(appStateSrc, "DistrictId");
                query.AddAttribute(query.Source, "Application_State");
                query.AddAttribute(appStateSrc, "Older_than_65");
                query.AddAttribute(appStateSrc, "BALL");
                query.AddAttribute(appStateSrc, "all");

                var table = new DataTable();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }
                /**********************************************************************************************/
                var items = new List<ReportItem>();
                double ball = GetBall(context, fd, ld);
                foreach (DataRow row in table.Rows)
                {
                    var appId = (Guid)row[0];
                    var stateId = (Guid)row[1];
                    var date = row[2] is DBNull ? DateTime.MinValue : (DateTime)row[2];
                    var regionId = row[3] is DBNull ? Guid.Empty : (Guid)row[3];
                    var districtId = row[4] is DBNull ? Guid.Empty : (Guid)row[4];
                    if (regionId == Guid.Empty || districtId == Guid.Empty) continue;
                    var appState = docRepo.LoadById((Guid)row[5]);
                    bool hasOlders = row[6] is DBNull ? false : ((int)row[6] > 0);
                    double val = row[7] is DBNull ? 0 : (double)row[7];
                    int memCount = row[8] is DBNull ? 0 : ((int)row[8]);
                    DateTime stateDate /*= row[8] is DBNull ? DateTime.MaxValue : (DateTime)row[8]*/;
                    bool hasDisabilities = Has2OrMoreDisabilities(context, appState);

                    var item = GetItem(items, regionId, districtId);
                    if (date >= fdLastYear && date < ldLastYear)//Количество получателей АСП за Прошлый год
                    {
                        var state = context.Documents.GetDocState(appId);
                        if (state == null) continue;
                        stateDate = state.Created ?? DateTime.MaxValue;
                        if ((stateId == ReadyAppointStateId && val <= ball) || stateId == AssignStateId || stateId == OnPaymentStateId)
                        {
                            item.ForLastYear = (item.ForLastYear ?? 0) + 1;
                            item.ForCurrentMonthAndYear = (item.ForCurrentMonthAndYear ?? 0) + 1;
                        }
                        else if (stateId == TerminatedStateId || stateId == TerminatedReturnStateId) //Количество заявлений которым было прекращено АСП в прошлом году
                        {
                            if (stateDate >= fd)
                            {
                                if (stateDate < ld) item.Terminated = (item.Terminated ?? 0) + 1;
                                else item.ForCurrentMonthAndYear = (item.ForCurrentMonthAndYear ?? 0) + 1;
                                item.ForLastYear = (item.ForLastYear ?? 0) + 1;
                            }
                        }
                        else if (stateId == CompletedStateId || stateId == DepositedStateId) //Количество семей для которых период получения АСП завершён в прошлом году
                        {
                            if (stateDate >= fd)
                            {
                                if (stateDate < ld) item.Completed = (item.Completed ?? 0) + 1;
                                else item.ForCurrentMonthAndYear = (item.ForCurrentMonthAndYear ?? 0) + 1;
                                item.ForLastYear = (item.ForLastYear ?? 0) + 1;
                            }
                        }
                    }
                    else if (date >= fd) //Ведённые заявления в АСП на текущий год
                    {
                        var stateList = context.Documents.GetDocumentStates(appId);
                        item.AllApplication = (item.AllApplication ?? 0) + 1;
                        var state = stateList.LastOrDefault(s => /*s.Created >= fd &&*/ s.Created < ld);
                        if (state == null) state = context.Documents.GetDocState(appId);
                        stateDate = state.Created ?? DateTime.MaxValue;

                        if (state.Type.Id == OnPaymentStateId || (state.Type.Id == ReadyAppointStateId && val <= ball) || state.Type.Id == AssignStateId)
                        {
                            item.Assigned = (item.Assigned ?? 0) + 1;
                            if (hasOlders && memCount == 1) item.Elderly = (item.Elderly ?? 0) + 1; //Одинокие престарелые
                            if (hasDisabilities) item.Disabled = (item.Disabled ?? 0) + 1; //Количество семей в составе которых 2 и более инвалидов
                            item.ForCurrentMonthAndYear = (item.ForCurrentMonthAndYear ?? 0) + 1;
                        }
                        else if ((state.Type.Id == ReadyAppointStateId && val > ball) || state.Type.Id == DeniedStateId) //Количество заявлений которым было отказано в АСП на текущем году
                        {
                            item.Denied = (item.Denied ?? 0) + 1;
                        }
                        else if (state.Type.Id == TerminatedStateId || state.Type.Id == TerminatedReturnStateId)
                        {
                            item.Terminated = (item.Terminated ?? 0) + 1; //Количество заявлений которым было прекращено АСП на текущем году
                            item.Assigned = (item.Assigned ?? 0) + 1;
                        }
                        else if (state.Type.Id == CompletedStateId || state.Type.Id == DepositedStateId)
                        {
                            item.Completed = (item.Completed ?? 0) + 1; //Количество семей для которых период получения АСП завершён
                            item.Assigned = (item.Assigned ?? 0) + 1;
                        }
                    }

                }

                foreach (var region in items.GroupBy(i => i.RegionId))
                {
                    var regionItem = new RegionItem();
                    regionItem.area = context.Documents.LoadById(region.Key)["Name"].ToString();
                    regionItem.Date = DateTime.Today;
                    regionItem.Year = year;
                    regionItem.Month = month;
                    var households = GetHouseholdByRegion(context, region.Key, fd, ld);
                    if (households.HasValue)
                        regionItem.AccordingToPlan = (int)Math.Round((decimal)(households.Value / 100) * 20);
                    regionItem.ForLastYear = (int)region.Sum(x => x.ForLastYear);
                    regionItem.AllApplication = (int)region.Sum(x => x.AllApplication);
                    regionItem.Assigned = (int)region.Sum(x => x.Assigned);
                    regionItem.Denied = (int)region.Sum(x => x.Denied);
                    regionItem.Terminated = (int)region.Sum(x => x.Terminated);
                    regionItem.Completed = (int)region.Sum(x => x.Completed);
                    regionItem.Elderly = (int)region.Sum(x => x.Elderly);
                    regionItem.Disabled = (int)region.Sum(x => x.Disabled);
                    regionItem.ForCurrentMonthAndYear = (int)region.Sum(x => x.ForCurrentMonthAndYear);

                    foreach (var district in region)
                    {
                        var districtItem = new DistrictItem();
                        if (district.DistrictId == Guid.Empty) continue;
                        districtItem.district = context.Documents.LoadById(district.DistrictId)["Name"].ToString();
                        districtItem.DateFormation = DateTime.Today;
                        households = GetHouseholdByDistrict(context, district.DistrictId, fd, ld);
                        if (households.HasValue)
                            districtItem.AccordingToPlan = (int)Math.Round((decimal)(households.Value / 100) * 20);
                        districtItem.ForLastYear = districtItem.ForLastYear;
                        districtItem.AllApplication = districtItem.AllApplication;
                        districtItem.Assigned = (int)district.Assigned;
                        districtItem.Denied = (int)district.Denied;
                        districtItem.Terminated = (int)district.Terminated;
                        districtItem.Completed = (int)district.Completed;
                        districtItem.Disabled = (int)district.Disabled;
                        districtItem.Elderly = (int)district.Elderly;
                        districtItem.ForCurrentMonthAndYear = (int)district.ForCurrentMonthAndYear;

                        if (households == null && district.ForLastYear == null && district.AllApplication == null && district.Assigned == null && district.Denied == null &&
                           district.Terminated == null && district.Completed == null && district.Disabled == null && district.Elderly == null && district.ForCurrentMonthAndYear == null)
                        {

                        }
                        else
                            regionItem.districtItems.Add(districtItem);
                    }
                    generalReportItem.regionItems.Add(regionItem);
                }
                return generalReportItem;
            }
            /****************************************************************************************************************/
            private static readonly Guid householdRegionDefId = new Guid("{D645C3A1-836C-4F79-824B-99B0FCF6835D}");
            private static readonly Guid householdDistrictDefId = new Guid("{0A47999A-6995-488E-9262-C680E9796028}");

            private static int? GetHouseholdByRegion(WorkflowContext context, Guid regionId, DateTime fd, DateTime ld)
            {
                var sqlQueryBuilder = context.Get<ISqlQueryBuilder>();
                var query = sqlQueryBuilder.Build(householdRegionDefId);
                query.AndCondition("Area", ConditionOperation.Equal, regionId);
                query.AndCondition("From", ConditionOperation.LessThen, ld.AddDays(1));
                query.AndCondition("To", ConditionOperation.GreatEqual, fd);
                query.AddAttribute("NumberOfHouseholds");
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    if (reader.Read())
                        return reader.GetInt32(0);
                }
                return null;
            }
            private static int? GetHouseholdByDistrict(WorkflowContext context, Guid districtId, DateTime fd, DateTime ld)
            {
                var sqlQueryBuilder = context.Get<ISqlQueryBuilder>();
                var query = sqlQueryBuilder.Build(householdDistrictDefId);
                query.AndCondition("District", ConditionOperation.Equal, districtId);
                query.AndCondition("From", ConditionOperation.LessThen, ld.AddDays(1));
                query.AndCondition("To", ConditionOperation.GreatEqual, fd);
                query.AddAttribute("NumHouseholds");
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    if (reader.Read())
                        return reader.GetInt32(0);
                }
                return null;
            }
            private static bool Has2OrMoreDisabilities(WorkflowContext context, Doc appState)
            {
                var disGroup = (Guid?)appState["Disability"] ?? Guid.Empty;
                var count = new Guid[] { disGroup1, disGroup2, disGroup3 }.Contains(disGroup) ? 1 : 0;
                var sqlQueryBuilder = context.Get<ISqlQueryBuilder>();
                var query = sqlQueryBuilder.BuildAttrList(appState, "Family_Member", "members");
                query.AndCondition("Disability", ConditionOperation.In, new object[] { disGroup1, disGroup2, disGroup3 });
                query.AddAttribute("&Id", SqlQuerySummaryFunction.Count);
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    if (reader.Read()) count += reader.GetInt32(0);
                }
                return count >= 2;
            }
            /****************************************************************************************************************/
            private static double GetBall(WorkflowContext context, DateTime fd, DateTime ld)
            {
                var ballDefId = new Guid("{E6133A73-23B7-4CCF-B144-5EF6A94308FA}");
                var qb = new QueryBuilder(ballDefId);
                qb.Where("From").Le(fd).And("To").Ge(ld);
                var query = context.CreateSqlQuery(qb.Def);
                query.AddAttribute("Value");
                using (var reader = context.CreateSqlReader(query))
                {
                    if (reader.Read())
                        return reader.IsDbNull(0) ? 0.0 : reader.GetDouble(0);
                }
                return 0.0;
            }
            /****************************************************************************************************************/
            private static ReportItem GetItem(List<ReportItem> items, Guid regionId, Guid districtId)
            {
                var item = items.FirstOrDefault(x => x.RegionId == regionId && x.DistrictId == districtId);
                if (item == null)
                {
                    item = new ReportItem { RegionId = regionId, DistrictId = districtId };
                    items.Add(item);
                }
                return item;
            }
            /****************************************************************************************************************/
            private class ReportItem
            {
                public Guid RegionId;
                public Guid DistrictId;
                //Za predydushiy god
                public int? ForLastYear;
                //Vsego za etot god
                public int? AllApplication;
                //iz nih naznacheny
                public int? Assigned;
                //iz nih otkazany
                public int? Denied;
                //iz nih prekrasheny
                public int? Terminated;
                //iz nih vyplaty zaversheny
                public int? Completed;
                //iz nih pojilye
                public int? Elderly;
                //iz nih imeyut 2 i bolee invalidov
                public int? Disabled;
                //iz nih deystvuyushie na tekushiy mesyac
                public int? ForCurrentMonthAndYear;
            }
            /****************************************************************************************************************/
            private static readonly Guid reportDefId = new Guid("{0EAF06CB-C021-479E-AA47-36D5E53484F7}");
            private static readonly Guid reportItemRegionDefId = new Guid("{2C358A76-1CE6-4C2E-9A80-C4A4745C0562}");
            private static readonly Guid reportItemDistrictDefId = new Guid("{E9B71030-1ABF-4C77-8CF3-7E3C879A29DF}");
            public static readonly Guid AppDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}");
            public static readonly Guid AppStateDefId = new Guid("{547BBA55-2281-4388-A1FC-EE890168AC2D}");
            private static readonly Guid assignmentDefId = new Guid("{51935CC6-CC48-4DAC-8853-DA8F57C057E8}");
            private static readonly Guid disGroup1 = new Guid("{11CA0BDE-5F53-4574-99BD-1B31C23B2662}");
            private static readonly Guid disGroup2 = new Guid("{374FA757-6C76-42C0-9EB7-AD2DDBC70889}");
            private static readonly Guid disGroup3 = new Guid("{D5D0A369-D793-47BA-B03B-A31CDB5EE351}");

            public static readonly Guid DeniedErrorTypeStateId = new Guid("{48BC65B8-0C18-4DEA-9948-DDCE279E3F0E}"); //Отказан по ошибке в вводе
            public static readonly Guid ReadyAppointStateId = new Guid("{0AE798A6-5471-4E2C-999E-7371799F6AD0}"); //Зарегистрирован
            public static readonly Guid AssignStateId = new Guid("{ACB44CC8-BF44-44F4-8056-723CED22536C}"); //Назначено
            public static readonly Guid OnPaymentStateId = new Guid("{78C294B5-B6EA-4075-9EEF-52073A6A2511}"); //На выплате
            public static readonly Guid DeniedStateId = new Guid("{5D8FF804-E287-41D5-8594-35A333F3CB49}"); //Отказано
            public static readonly Guid TerminatedStateId = new Guid("{9D5EFFDB-7389-4E59-9490-BD57D7D94AB1}"); //Прекращен
            public static readonly Guid TerminatedReturnStateId = new Guid("{0B0C57CD-DFF0-4DBB-BCCA-7128A58D018B}"); //Прекращен с возвратом
            public static readonly Guid CompletedStateId = new Guid("{62D08FE4-B847-4591-A7C9-E113E0E60BC3}"); //Завершен
            public static readonly Guid DepositedStateId = new Guid("{080E140C-943A-49A3-962A-6E892A58D7BE}"); //Завершен с депонированием 

            [DataContract]
            public class RegionItem
            {
                [DataMember]
                public int Elderly { get; set; }
                [DataMember]
                public int AllApplication { get; set; }
                [DataMember]
                public string area { get; set; }
                [DataMember]
                public List<DistrictItem> districtItems { get; set; }
                [DataMember]
                public DateTime Date { get; set; }
                [DataMember]
                public RecipientsOverPastCurrentYearMonth recipientsOverPastCurrentYearMonth { get; set; }
                [DataMember]
                public int Assigned { get; set; }
                [DataMember]
                public int Month { get; set; }
                [DataMember]
                public int AccordingToPlan { get; set; }
                [DataMember]
                public int Completed { get; set; }
                [DataMember]
                public int Denied { get; set; }
                [DataMember]
                public int Terminated { get; set; }
                [DataMember]
                public int Year { get; set; }
                [DataMember]
                public int ForLastYear { get; set; }
                [DataMember]
                public int ForCurrentMonthAndYear { get; set; }
                [DataMember]
                public int Disabled { get; set; }
                public RegionItem()
                {
                    recipientsOverPastCurrentYearMonth = new RecipientsOverPastCurrentYearMonth();
                    districtItems = new List<DistrictItem>();
                }
            }

            [DataContract]
            public class DistrictItem
            {
                [DataMember]
                public int Elderly { get; set; }
                [DataMember]
                public int AllApplication { get; set; }
                [DataMember]
                public string district { get; set; }
                [DataMember]
                public int Assigned { get; set; }
                [DataMember]
                public int AccordingToPlan { get; set; }
                [DataMember]
                public int Completed { get; set; }
                [DataMember]
                public int Denied { get; set; }
                [DataMember]
                public int Terminated { get; set; }
                [DataMember]
                public int ForLastYear { get; set; }
                [DataMember]
                public int ForCurrentMonthAndYear { get; set; }
                [DataMember]
                public int Disabled { get; set; }
                [DataMember]
                public DateTime DateFormation { get; set; }
            }

            [DataContract]
            public class RecipientsOverPastCurrentYearMonth
            {
                [DataMember]
                public DateTime Date { get; set; }
                [DataMember]
                public int Month { get; set; }
                [DataMember]
                public int Year { get; set; }
                [DataMember]
                public string Note { get; set; }
            }

            [DataContract]
            public class GeneralReportItem
            {
                [DataMember]
                public DateTime Date { get; set; }
                [DataMember]
                public int Year { get; set; }
                [DataMember]
                public int Month { get; set; }
                [DataMember]
                public List<RegionItem> regionItems { get; set; }

                public GeneralReportItem()
                {
                    regionItems = new List<RegionItem>();
                }
            }
        }

        public static class Report_116
        {
            //Отчет о верификации
            public static VerificationReport Execute(DateTime dateFormation)
            {
                var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));
                var docRepo = context.Documents;
                var report = new VerificationReport();

                var date = dateFormation;

                if (date == DateTime.MinValue)
                    throw new ApplicationException("Ошибка в периоде!");

                var q = GetQuarter(date.Month);
                var fd = new DateTime(date.Year, 1, 1);
                var ld = LDByQuarter(q, date.Year);

                var items = new List<ReportItem>();
                var ui = context.GetUserInfo();
                //all app
                /****************************************************************************************************************/
                var qb = new QueryBuilder(AppDefId, context.UserId);
                qb.Where("&OrgId").Eq(ui.OrganizationId).And("Date").Ge(fd).And("Date").Le(ld).And("&State").Neq(ErrorAppStateId);
                using (var query = SqlQueryBuilder.Build(context.DataContext, qb.Def))
                {
                    var appStateSrc = query.JoinSource(query.Source, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");

                    var regAttr = query.AddAttribute(appStateSrc, "RegionId");
                    var disAttr = query.AddAttribute(appStateSrc, "DistrictId");
                    var djamAttr = query.AddAttribute(appStateSrc, "DjamoatId");
                    var mAttr = query.AddAttribute(query.Source, "Date");

                    query.AddAttribute("&Id", SqlQuerySummaryFunction.Count);
                    query.AddGroupAttribute(regAttr);
                    query.AddGroupAttribute(disAttr);
                    query.AddGroupAttribute(djamAttr);
                    query.AddGroupAttribute(mAttr);

                    using (var reader = new SqlQueryReader(context.DataContext, query))
                    {
                        while (reader.Read())
                        {
                            var regionId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                            var districtId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                            var djamoatId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                            if (regionId == Guid.Empty || districtId == Guid.Empty || djamoatId == Guid.Empty)
                                continue;
                            var m = reader.GetDateTime(3);
                            var count = reader.IsDbNull(4) ? 0 : reader.GetInt32(4);

                            var item = GetItem(items, regionId, districtId, djamoatId);

                            if (GetQuarter(m.Month) == 1)
                                item.AllAppCount1 = ((int?)item.AllAppCount1 ?? 0) + count;
                            else if (GetQuarter(m.Month) == 2)
                                item.AllAppCount2 = ((int?)item.AllAppCount2 ?? 0) + count;
                            else if (GetQuarter(m.Month) == 3)
                                item.AllAppCount3 = ((int?)item.AllAppCount3 ?? 0) + count;
                            else if (GetQuarter(m.Month) == 4)
                                item.AllAppCount4 = ((int?)item.AllAppCount4 ?? 0) + count;
                        }
                    }
                }
                /*********************************************************************************************************/
                //назначенный и на выплате
                /****************************************************************************************************************/
                qb = new QueryBuilder(BankAccountDefId, context.UserId);
                qb.Where("&OrgId").Eq(ui.OrganizationId).And("Application").Include("Date").Ge(fd).And("Date").Le(ld).End();
                var sqlQueryBuilder = context.Get<ISqlQueryBuilder>();
                using (var query = sqlQueryBuilder.Build(qb.Def))
                {
                    var appSrc = query.JoinSource(query.Source, AppDefId, SqlSourceJoinType.Inner, "Application");
                    var appStateSrc = query.JoinSource(appSrc, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");

                    var regAttr = query.AddAttribute(appStateSrc, "RegionId");
                    var disAttr = query.AddAttribute(appStateSrc, "DistrictId");
                    var djamAttr = query.AddAttribute(appStateSrc, "DjamoatId");
                    var mAttr = query.AddAttribute(appSrc, "Date");
                    query.AddAttribute("&Id", SqlQuerySummaryFunction.Count);
                    query.AddGroupAttribute(regAttr);
                    query.AddGroupAttribute(disAttr);
                    query.AddGroupAttribute(djamAttr);
                    query.AddGroupAttribute(mAttr);

                    using (var reader = new SqlQueryReader(context.DataContext, query))
                    {
                        while (reader.Read())
                        {
                            var regionId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                            var districtId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                            var djamoatId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                            if (regionId == Guid.Empty || districtId == Guid.Empty || djamoatId == Guid.Empty)
                                continue;
                            var m = reader.GetDateTime(3);
                            var count = reader.IsDbNull(4) ? 0 : reader.GetInt32(4);

                            var item = GetItem(items, regionId, districtId, djamoatId);
                            if (GetQuarter(m.Month) == 1)
                                item.AssignedPaidCount1 = ((int?)item.AssignedPaidCount1 ?? 0) + count;
                            else if (GetQuarter(m.Month) == 2)
                                item.AssignedPaidCount2 = ((int?)item.AssignedPaidCount2 ?? 0) + count;
                            else if (GetQuarter(m.Month) == 3)
                                item.AssignedPaidCount3 = ((int?)item.AssignedPaidCount3 ?? 0) + count;
                            else if (GetQuarter(m.Month) == 4)
                                item.AssignedPaidCount4 = ((int?)item.AssignedPaidCount4 ?? 0) + count;
                        }
                    }
                }
                /*********************************************************************************************************/
                //верификация
                /****************************************************************************************************************/
                qb = new QueryBuilder(InspectionActId, context.UserId);
                qb.Where("&OrgId").Eq(ui.OrganizationId).And("&State").Neq(RegistrationStateId).And("CreatedSite").Eq(new Guid("{88B0001B-435D-453A-9EAA-DB6FDF8193B5}"));
                using (var query = SqlQueryBuilder.Build(context.DataContext, qb.Def))
                {
                    var appSrc = query.JoinSource(query.Source, AppDefId, SqlSourceJoinType.Inner, "Application");
                    var appStateSrc = query.JoinSource(query.Source, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");

                    var regAttr = query.AddAttribute(appStateSrc, "RegionId");
                    var disAttr = query.AddAttribute(appStateSrc, "DistrictId");
                    var djamAttr = query.AddAttribute(appStateSrc, "DjamoatId");
                    var mAttr = query.AddAttribute(appSrc, "Date");
                    query.AddAttribute("&Id", SqlQuerySummaryFunction.Count);

                    query.AddGroupAttribute(regAttr);
                    query.AddGroupAttribute(disAttr);
                    query.AddGroupAttribute(djamAttr);
                    query.AddGroupAttribute(mAttr);

                    using (var reader = new SqlQueryReader(context.DataContext, query))
                    {
                        while (reader.Read())
                        {
                            var regionId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                            var districtId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                            var djamoatId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                            if (regionId == Guid.Empty || districtId == Guid.Empty || djamoatId == Guid.Empty)
                                continue;
                            var m = !reader.IsDbNull(3) ? reader.GetDateTime(3) : DateTime.MinValue;
                            var count = reader.IsDbNull(4) ? 0 : reader.GetInt32(4);
                            var item = GetItem(items, regionId, districtId, djamoatId);
                            if (m.Year != fd.Year) continue;

                            {
                                if (GetQuarter(m.Month) == 1)
                                    item.VerificationCount1 = ((int?)item.VerificationCount1 ?? 0) + count;
                                else if (GetQuarter(m.Month) == 2)
                                    item.VerificationCount2 = ((int?)item.VerificationCount2 ?? 0) + count;
                                else if (GetQuarter(m.Month) == 3)
                                    item.VerificationCount3 = ((int?)item.VerificationCount3 ?? 0) + count;
                                else if (GetQuarter(m.Month) == 4)
                                    item.VerificationCount4 = ((int?)item.VerificationCount4 ?? 0) + count;
                            }
                        }
                    }
                }
                /*********************************************************************************************************/
                var AppCount = 0;
                foreach (var regions in items.GroupBy(x => x.RegionId))
                {
                    var regionItem = new AreaReport();
                    regionItem.DateFormation = date;
                    regionItem.area = context.Documents.LoadById(regions.Key)["Name"].ToString();
                    regionItem.Year = date.Year;
                    regionItem.Month = date.Month;
                    var households = GetHouseholdByRegion(context, regions.Key, fd, ld);
                    if (households.HasValue)
                        regionItem.NumberOfHouseholds = households.Value;
                    var regAllApp1 = 0;
                    var regAllApp2 = 0;
                    var regAllApp3 = 0;
                    var regAllApp4 = 0;

                    var regAssPaid1 = 0;
                    var regAssPaid2 = 0;
                    var regAssPaid3 = 0;
                    var regAssPaid4 = 0;

                    var regVerification1 = 0;
                    var regVerification2 = 0;
                    var regVerification3 = 0;
                    var regVerification4 = 0;
                    foreach (var districts in regions.GroupBy(x => x.DistrictId))
                    {
                        var districtItem = new DistrictReport();
                        districtItem.DateFormation = date;
                        districtItem.district = context.Documents.LoadById(districts.Key)["Name"].ToString();
                        districtItem.Year = date.Year;
                        districtItem.Month = date.Month;

                        households = GetHouseholdByDistrict(context, districts.Key, fd, ld);
                        if (households.HasValue)
                            districtItem.NumberOfHouseholds = households.Value;
                        var disAllApp1 = 0;
                        var disAllApp2 = 0;
                        var disAllApp3 = 0;
                        var disAllApp4 = 0;

                        var disAssPaid1 = 0;
                        var disAssPaid2 = 0;
                        var disAssPaid3 = 0;
                        var disAssPaid4 = 0;

                        var disVerification1 = 0;
                        var disVerification2 = 0;
                        var disVerification3 = 0;
                        var disVerification4 = 0;

                        foreach (var item in districts)
                        {
                            var djamoatItem = new DjamoatReport();
                            djamoatItem.DateFormation = date;
                            djamoatItem.djamoat = context.Documents.LoadById(item.DjamoatId)["Name"].ToString();
                            djamoatItem.Year = date.Year;
                            djamoatItem.Month = date.Month;
                            households = GetHouseholdByDjamoat(context, item.DjamoatId, fd, ld);
                            if (households.HasValue)
                                djamoatItem.NumberOfHouseholds = households.Value;
                            var AllApp1 = item.AllAppCount1 ?? 0;
                            var AllApp2 = item.AllAppCount2 ?? 0;
                            var AllApp3 = item.AllAppCount3 ?? 0;
                            var AllApp4 = item.AllAppCount4 ?? 0;

                            var AssPaid1 = item.AssignedPaidCount1 ?? 0;
                            var AssPaid2 = item.AssignedPaidCount2 ?? 0;
                            var AssPaid3 = item.AssignedPaidCount3 ?? 0;
                            var AssPaid4 = item.AssignedPaidCount4 ?? 0;

                            var Verification1 = item.VerificationCount1 ?? 0;
                            var Verification2 = item.VerificationCount2 ?? 0;
                            var Verification3 = item.VerificationCount3 ?? 0;
                            var Verification4 = item.VerificationCount4 ?? 0;

                            djamoatItem.DjamAppeal_1 = AllApp1;
                            djamoatItem.DjamAppeal_2 = AllApp2;
                            djamoatItem.DjamAppeal_3 = AllApp3;
                            djamoatItem.DjamAppeal_4 = AllApp4;

                            djamoatItem.DjamAppointed_1 = AssPaid1;
                            djamoatItem.DjamAppointed_2 = AssPaid2;
                            djamoatItem.DjamAppointed_3 = AssPaid3;
                            djamoatItem.DjamAppointed_4 = AssPaid4;

                            djamoatItem.DjamVerified_1 = Verification1;
                            djamoatItem.DjamVerified_2 = Verification2;
                            djamoatItem.DjamVerified_3 = Verification3;
                            djamoatItem.DjamVerified_4 = Verification4;


                            if (AssPaid1 > 0) djamoatItem.DjamPercentage_1 = Verification1 * 100 / AssPaid1;
                            if (AssPaid2 > 0) djamoatItem.DjamPercentage_2 = Verification2 * 100 / AssPaid2;
                            if (AssPaid3 > 0) djamoatItem.DjamPercentage_3 = Verification3 * 100 / AssPaid3;
                            if (AssPaid4 > 0) djamoatItem.DjamPercentage_4 = Verification4 * 100 / AssPaid4;

                            districtItem.djamoatReports.Add(djamoatItem);

                            disAllApp1 += AllApp1;
                            disAllApp2 += AllApp2;
                            disAllApp3 += AllApp3;
                            disAllApp4 += AllApp4;

                            disAssPaid1 += AssPaid1;
                            disAssPaid2 += AssPaid2;
                            disAssPaid3 += AssPaid3;
                            disAssPaid4 += AssPaid4;

                            disVerification1 += Verification1;
                            disVerification2 += Verification2;
                            disVerification3 += Verification3;
                            disVerification4 += Verification4;
                        }

                        districtItem.DisAppeal_1 = disAllApp1;
                        districtItem.DisAppeal_2 = disAllApp2;
                        districtItem.DisAppeal_3 = disAllApp3;
                        districtItem.DisAppeal_4 = disAllApp4;

                        districtItem.DisAppointed_1 = disAssPaid1;
                        districtItem.DisAppointed_2 = disAssPaid2;
                        districtItem.DisAppointed_3 = disAssPaid3;
                        districtItem.DisAppointed_4 = disAssPaid4;

                        districtItem.DisVerified_1 = disVerification1;
                        districtItem.DisVerified_2 = disVerification2;
                        districtItem.DisVerified_3 = disVerification3;
                        districtItem.DisVerified_4 = disVerification4;

                        if (disAssPaid1 > 0) districtItem.DisPercentage_1 = disVerification1 * 100 / disAssPaid1;
                        if (disAssPaid2 > 0) districtItem.DisPercentage_2 = disVerification2 * 100 / disAssPaid2;
                        if (disAssPaid3 > 0) districtItem.DisPercentage_3 = disVerification3 * 100 / disAssPaid3;
                        if (disAssPaid4 > 0) districtItem.DisPercentage_4 = disVerification4 * 100 / disAssPaid4;

                        regionItem.districtReports.Add(districtItem);

                        regAllApp1 += disAllApp1;
                        regAllApp2 += disAllApp2;
                        regAllApp3 += disAllApp3;
                        regAllApp4 += disAllApp4;

                        regAssPaid1 += disAssPaid1;
                        regAssPaid2 += disAssPaid2;
                        regAssPaid3 += disAssPaid3;
                        regAssPaid4 += disAssPaid4;

                        regVerification1 += disVerification1;
                        regVerification2 += disVerification2;
                        regVerification3 += disVerification3;
                        regVerification4 += disVerification4;
                    }

                    regionItem.Appeal_1 = regAllApp1;
                    regionItem.Appeal_2 = regAllApp2;
                    regionItem.Appeal_3 = regAllApp3;
                    regionItem.Appeal_4 = regAllApp4;

                    regionItem.Appointed_1 = regAssPaid1;
                    regionItem.Appointed_2 = regAssPaid2;
                    regionItem.Appointed_3 = regAssPaid3;
                    regionItem.Appointed_4 = regAssPaid4;

                    regionItem.Verified_1 = regVerification1;
                    regionItem.Verified_2 = regVerification1;
                    regionItem.Verified_3 = regVerification1;
                    regionItem.Verified_4 = regVerification1;


                    if (regAssPaid1 > 0) regionItem.Percentage_1 = regVerification1 * 100 / regAssPaid1;
                    if (regAssPaid2 > 0) regionItem.Percentage_2 = regVerification2 * 100 / regAssPaid2;
                    if (regAssPaid3 > 0) regionItem.Percentage_3 = regVerification3 * 100 / regAssPaid3;
                    if (regAssPaid4 > 0) regionItem.Percentage_4 = regVerification4 * 100 / regAssPaid4;

                    AppCount++;
                    report.areaReports.Add(regionItem);
                }
                report.AppCount = AppCount;
                return report;
            }

            private static readonly Guid reportDefId = new Guid("{2CAFEA8D-7B53-4A14-8E00-361DE45C2401}");
            private static readonly Guid reportItemRegionDefId = new Guid("{E00BFFCC-1B56-4192-9B07-95C5025CD9AC}"); //по областям
            private static readonly Guid reportItemDistrictDefId = new Guid("{CCFADA4E-DF7B-4E5A-81D9-6DF70B58A444}"); //по районам
            private static readonly Guid reportItemDjamoatDefId = new Guid("{3DC972FE-E986-4F04-86D0-D6A0A1832722}"); //по джамоатам

            private static readonly Guid AppDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}");
            private static readonly Guid AppStateDefId = new Guid("{547BBA55-2281-4388-A1FC-EE890168AC2D}");
            private static readonly Guid VerificationId = new Guid("{FCE8FE12-1E61-48C6-8553-47E967EA8C34}"); //Верификация
            private static readonly Guid InspectionActId = new Guid("{ABBD8AAA-5324-4527-B59B-E2719C024AAC}"); //Акт проверки домохозяйства
            private static readonly Guid BankAccountDefId = new Guid("{BE6D5C1F-48A6-483B-980A-14CEFF781FD4}"); //Банковский счет

            private static readonly Guid ErrorAppStateId = new Guid("{48BC65B8-0C18-4DEA-9948-DDCE279E3F0E}"); //Отказан по ошибке в вводе
            private static readonly Guid AssignedAppStateId = new Guid("{ACB44CC8-BF44-44F4-8056-723CED22536C}"); //Назначено  
            private static readonly Guid OnPaymentStateId = new Guid("{78C294B5-B6EA-4075-9EEF-52073A6A2511}"); //На выплате
            private static readonly Guid RegistrationStateId = new Guid("{F062290E-42E3-4E24-8547-9FA7606547D7}"); //На регистрации

            private static ReportItem GetItem(List<ReportItem> items, Guid regionId, Guid districtId, Guid djamoatId)
            {
                var item = items.FirstOrDefault(x => x.RegionId == regionId && x.DistrictId == districtId && x.DjamoatId == djamoatId);
                if (item != null) return item;
                var newItem = new ReportItem { RegionId = regionId, DistrictId = districtId, DjamoatId = djamoatId };
                items.Add(newItem);
                return newItem;
            }

            private static int GetQuarter(int m)
            {
                if (m <= 3) return 1;
                else if (m > 3 && m <= 6) return 2;
                else if (m > 6 && m <= 9) return 3;
                else if (m > 9 && m <= 12) return 4;
                throw new ApplicationException("Квартал не определен!");
            }

            private static DateTime FDByQuarter(int q, int y)
            {
                if (q == 1) return new DateTime(y, 1, 1);
                else if (q == 2) return new DateTime(y, 4, 1);
                else if (q == 3) return new DateTime(y, 7, 1);
                else if (q == 4) return new DateTime(y, 10, 1);
                throw new ApplicationException("Дата начала не определена!");
            }

            private static DateTime LDByQuarter(int q, int y)
            {
                if (q == 1) return new DateTime(y, 1, DateTime.DaysInMonth(y, 1));
                else if (q == 2) return new DateTime(y, 4, DateTime.DaysInMonth(y, 4));
                else if (q == 3) return new DateTime(y, 7, DateTime.DaysInMonth(y, 7));
                else if (q == 4) return new DateTime(y, 10, DateTime.DaysInMonth(y, 10));
                throw new ApplicationException("Дата начала не определена!");
            }
            private static readonly Guid householdRegionDefId = new Guid("{D645C3A1-836C-4F79-824B-99B0FCF6835D}");
            private static readonly Guid householdDistrictDefId = new Guid("{0A47999A-6995-488E-9262-C680E9796028}");
            private static readonly Guid householdDjamoatDefId = new Guid("{F6B800EA-C6FA-46A1-B663-141E8201A108}");

            private static int? GetHouseholdByRegion(WorkflowContext context, Guid regionId, DateTime fd, DateTime ld)
            {
                var sqlQueryBuilder = context.Get<ISqlQueryBuilder>();
                var query = sqlQueryBuilder.Build(householdRegionDefId);
                query.AndCondition("Area", ConditionOperation.Equal, regionId);
                query.AndCondition("From", ConditionOperation.LessThen, ld.AddDays(1));
                query.AndCondition("To", ConditionOperation.GreatEqual, fd);
                query.AddAttribute("NumberOfHouseholds");
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    if (reader.Read())
                        return reader.GetInt32(0);
                }
                return null;
            }
            private static int? GetHouseholdByDistrict(WorkflowContext context, Guid districtId, DateTime fd, DateTime ld)
            {
                var sqlQueryBuilder = context.Get<ISqlQueryBuilder>();
                var query = sqlQueryBuilder.Build(householdDistrictDefId);
                query.AndCondition("District", ConditionOperation.Equal, districtId);
                query.AndCondition("From", ConditionOperation.LessThen, ld.AddDays(1));
                query.AndCondition("To", ConditionOperation.GreatEqual, fd);
                query.AddAttribute("NumHouseholds");
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    if (reader.Read())
                        return reader.GetInt32(0);
                }
                return null;
            }
            private static int? GetHouseholdByDjamoat(WorkflowContext context, Guid djamoatId, DateTime fd, DateTime ld)
            {
                var sqlQueryBuilder = context.Get<ISqlQueryBuilder>();
                var query = sqlQueryBuilder.Build(householdDjamoatDefId);
                query.AndCondition("Djamoat_Doc", ConditionOperation.Equal, djamoatId);
                query.AndCondition("From", ConditionOperation.LessThen, ld.AddDays(1));
                query.AndCondition("To", ConditionOperation.GreatEqual, fd);
                query.AddAttribute("Percentage");
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    if (reader.Read())
                        return reader.GetInt32(0);
                }
                return null;
            }

            [DataContract]
            public class AreaReport
            {
                [DataMember]
                public int Year { get; set; }
                [DataMember]
                public int NumberOfHouseholds { get; set; }
                [DataMember]
                public int Month { get; set; }
                [DataMember]
                public DateTime DateFormation { get; set; }
                [DataMember]
                public string area { get; set; }
                [DataMember]
                public List<DistrictReport> districtReports { get; set; }
                [DataMember]
                public int Appeal_1 { get; set; }
                [DataMember]
                public int Appeal_2 { get; set; }
                [DataMember]
                public int Appeal_3 { get; set; }
                [DataMember]
                public int Appeal_4 { get; set; }
                [DataMember]
                public int Percentage_1 { get; set; }
                [DataMember]
                public int Percentage_2 { get; set; }
                [DataMember]
                public int Percentage_3 { get; set; }
                [DataMember]
                public int Percentage_4 { get; set; }
                [DataMember]
                public int Verified_1 { get; set; }
                [DataMember]
                public int Verified_2 { get; set; }
                [DataMember]
                public int Verified_3 { get; set; }
                [DataMember]
                public int Verified_4 { get; set; }
                [DataMember]
                public int Appointed_1 { get; set; }
                [DataMember]
                public int Appointed_2 { get; set; }
                [DataMember]
                public int Appointed_3 { get; set; }
                [DataMember]
                public int Appointed_4 { get; set; }
                public AreaReport()
                {
                    districtReports = new List<DistrictReport>();
                }
            }

            [DataContract]
            public class DistrictReport
            {
                [DataMember]
                public int Year { get; set; }
                [DataMember]
                public int NumberOfHouseholds { get; set; }
                [DataMember]
                public int Month { get; set; }
                [DataMember]
                public DateTime DateFormation { get; set; }
                [DataMember]
                public string district { get; set; }
                [DataMember]
                public List<DjamoatReport> djamoatReports { get; set; }
                [DataMember]
                public int DisAppeal_1 { get; set; }
                [DataMember]
                public int DisAppeal_2 { get; set; }
                [DataMember]
                public int DisAppeal_3 { get; set; }
                [DataMember]
                public int DisAppeal_4 { get; set; }
                [DataMember]
                public int DisPercentage_1 { get; set; }
                [DataMember]
                public int DisPercentage_2 { get; set; }
                [DataMember]
                public int DisPercentage_3 { get; set; }
                [DataMember]
                public int DisPercentage_4 { get; set; }
                [DataMember]
                public int DisVerified_1 { get; set; }
                [DataMember]
                public int DisVerified_2 { get; set; }
                [DataMember]
                public int DisVerified_3 { get; set; }
                [DataMember]
                public int DisVerified_4 { get; set; }
                [DataMember]
                public int DisAppointed_1 { get; set; }
                [DataMember]
                public int DisAppointed_2 { get; set; }
                [DataMember]
                public int DisAppointed_3 { get; set; }
                [DataMember]
                public int DisAppointed_4 { get; set; }

                public DistrictReport()
                {
                    djamoatReports = new List<DjamoatReport>();
                }
            }

            [DataContract]
            public class DjamoatReport
            {
                [DataMember]
                public int Year { get; set; }
                [DataMember]
                public int NumberOfHouseholds { get; set; }
                [DataMember]
                public int Month { get; set; }
                [DataMember]
                public DateTime DateFormation { get; set; }
                [DataMember]
                public string djamoat { get; set; }
                [DataMember]
                public int DjamAppeal_1 { get; set; }
                [DataMember]
                public int DjamAppeal_2 { get; set; }
                [DataMember]
                public int DjamAppeal_3 { get; set; }
                [DataMember]
                public int DjamAppeal_4 { get; set; }
                [DataMember]
                public int DjamPercentage_1 { get; set; }
                [DataMember]
                public int DjamPercentage_2 { get; set; }
                [DataMember]
                public int DjamPercentage_3 { get; set; }
                [DataMember]
                public int DjamPercentage_4 { get; set; }
                [DataMember]
                public int DjamVerified_1 { get; set; }
                [DataMember]
                public int DjamVerified_2 { get; set; }
                [DataMember]
                public int DjamVerified_3 { get; set; }
                [DataMember]
                public int DjamVerified_4 { get; set; }
                [DataMember]
                public int DjamAppointed_1 { get; set; }
                [DataMember]
                public int DjamAppointed_2 { get; set; }
                [DataMember]
                public int DjamAppointed_3 { get; set; }
                [DataMember]
                public int DjamAppointed_4 { get; set; }

            }

            [DataContract]
            public class VerificationReport
            {
                [DataMember]
                public DateTime DateFormation { get; set; }
                [DataMember]
                public int Year { get; set; }
                [DataMember]
                public int Month { get; set; }
                [DataMember]
                public string Note { get; set; }
                [DataMember]
                public List<AreaReport> areaReports { get; set; }
                [DataMember]
                public int AppCount { get; set; }
                public VerificationReport()
                {
                    areaReports = new List<AreaReport>();
                }
            }

            private class ReportItem
            {
                public Guid RegionId;
                public Guid DistrictId;
                public Guid DjamoatId;
                public int? AllAppCount1;
                public int? AllAppCount2;
                public int? AllAppCount3;
                public int? AllAppCount4;
                public int? AssignedPaidCount1;
                public int? AssignedPaidCount2;
                public int? AssignedPaidCount3;
                public int? AssignedPaidCount4;
                public int? VerificationCount1;
                public int? VerificationCount2;
                public int? VerificationCount3;
                public int? VerificationCount4;

            }
        }

        public static class Report_117
        {
            //Банковский отчет
            public static BankingReport Execute(DateTime DateFormation)
            {
                var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));
                var docRepo = context.Documents;
                var report = new BankingReport();
                var date = DateFormation;

                if (date == DateTime.MinValue)
                    throw new ApplicationException("Ошибка в периоде!");

                var q = GetQuarter(date.Month);
                var fd = new DateTime(date.Year, 1, 1);
                var ld = LDByQuarter(q, date.Year);

                var items = new List<ReportItem>();
                var ui = context.GetUserInfo();
                //назначенный и на выплате
                var qb = new QueryBuilder(BankAccountDefId, context.UserId);
                qb.Where("&OrgId").Eq(ui.OrganizationId).And("Application").Include/*("&InState").In(new object[] { AssignedAppStateId, OnPaymentStateId }).And*/("Date").Ge(fd).And("Date").Le(ld).End();
                var sqlQueryBuilder = context.Get<ISqlQueryBuilder>();
                using (var query = sqlQueryBuilder.Build(qb.Def))
                {
                    var appSrc = query.JoinSource(query.Source, AppDefId, SqlSourceJoinType.Inner, "Application");
                    var appStateSrc = query.JoinSource(appSrc, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");

                    var regAttr = query.AddAttribute(appStateSrc, "RegionId");
                    var disAttr = query.AddAttribute(appStateSrc, "DistrictId");
                    var djamAttr = query.AddAttribute(appStateSrc, "DjamoatId");
                    var mAttr = query.AddAttribute(appSrc, "Date");
                    query.AddAttribute("&Id", SqlQuerySummaryFunction.Count);
                    query.AddGroupAttribute(regAttr);
                    query.AddGroupAttribute(disAttr);
                    query.AddGroupAttribute(djamAttr);
                    query.AddGroupAttribute(mAttr);
                    using (var reader = new SqlQueryReader(context.DataContext, query))
                    {
                        while (reader.Read())
                        {
                            var regionId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                            var districtId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                            var djamoatId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                            if (regionId == Guid.Empty || districtId == Guid.Empty || djamoatId == Guid.Empty)
                                continue;
                            var m = reader.GetDateTime(3);
                            var count = reader.IsDbNull(4) ? 0 : reader.GetInt32(4);

                            var item = GetItem(items, regionId, districtId, djamoatId);
                            if (GetQuarter(m.Month) == 1)
                                item.AssignedCount1 = ((int?)item.AssignedCount1 ?? 0) + count;
                            else if (GetQuarter(m.Month) == 2)
                                item.AssignedCount2 = ((int?)item.AssignedCount2 ?? 0) + count;
                            else if (GetQuarter(m.Month) == 3)
                                item.AssignedCount3 = ((int?)item.AssignedCount3 ?? 0) + count;
                            else if (GetQuarter(m.Month) == 4)
                                item.AssignedCount4 = ((int?)item.AssignedCount4 ?? 0) + count;
                        }
                    }
                }
                //гр.2
                qb = new QueryBuilder(BankAccTernDefId, context.UserId);
                qb.Where("&OrgId").Eq(ui.OrganizationId).And("Report").Include("LoadDate").Le(ld).And("LoadDate").Ge(fd).End();
                using (var query = sqlQueryBuilder.Build(qb.Def))
                {
                    var bankAccountSrc = query.JoinSource(query.Source, BankAccountDefId, SqlSourceJoinType.Inner, "BankAccount");
                    var appSrc = query.JoinSource(bankAccountSrc, AppDefId, SqlSourceJoinType.Inner, "Application");
                    var appStateSrc = query.JoinSource(appSrc, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");
                    var bankTurnOverReportSrc = query.FindSourceById(bankTurnowerRepoDefId);
                    var regAttr = query.AddAttribute(appStateSrc, "RegionId");
                    var disAttr = query.AddAttribute(appStateSrc, "DistrictId");
                    var djamAttr = query.AddAttribute(appStateSrc, "DjamoatId");
                    var dateAttr = query.AddAttribute(bankTurnOverReportSrc, "LoadDate");
                    query.AddAttribute("&Id", SqlQuerySummaryFunction.Count);
                    var appAttr = query.AddAttribute(appSrc, "&Id");
                    query.AddGroupAttribute(regAttr);
                    query.AddGroupAttribute(disAttr);
                    query.AddGroupAttribute(djamAttr);
                    query.AddGroupAttribute(dateAttr);
                    query.AddGroupAttribute(appAttr);
                    var appList = new List<Guid>();
                    using (var reader = new SqlQueryReader(context.DataContext, query))
                    {
                        while (reader.Read())
                        {
                            var appId = reader.GetGuid(5);
                            if (appList.Contains(appId))
                                continue;
                            else
                                appList.Add(appId);
                            var regionId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                            var districtId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                            var djamoatId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                            if (regionId == Guid.Empty || districtId == Guid.Empty || djamoatId == Guid.Empty)
                                continue;
                            var loadDate = reader.GetDateTime(3);
                            var count = reader.IsDbNull(4) ? 0 : reader.GetInt32(4);

                            var item = GetItem(items, regionId, districtId, djamoatId);
                            if (GetQuarter(loadDate.Month) == 1)
                                item.PaidCount1 = ((int?)item.PaidCount1 ?? 0) + count;
                            else if (GetQuarter(loadDate.Month) == 2)
                                item.PaidCount2 = ((int?)item.PaidCount2 ?? 0) + count;
                            else if (GetQuarter(loadDate.Month) == 3)
                                item.PaidCount3 = ((int?)item.PaidCount3 ?? 0) + count;
                            else if (GetQuarter(loadDate.Month) == 4)
                                item.PaidCount4 = ((int?)item.PaidCount4 ?? 0) + count;
                        }
                    }
                }
                //гр.3
                qb = new QueryBuilder(NonPaymentDefId, context.UserId);
                qb.Where("&OrgId").Eq(ui.OrganizationId).And("Report").Include("LoadDate").Le(ld).And("LoadDate").Ge(new DateTime(date.Year, 1, 1)).End();
                using (var query = sqlQueryBuilder.Build(qb.Def))
                {
                    var bankAccountSrc = query.JoinSource(query.Source, BankAccountDefId, SqlSourceJoinType.Inner, "BankAccount");
                    var appSrc = query.JoinSource(bankAccountSrc, AppDefId, SqlSourceJoinType.Inner, "Application");
                    var appStateSrc = query.JoinSource(appSrc, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");
                    var nonPaymentReportSrc = query.FindSourceById(NonPaymentReportDefId);
                    var regAttr = query.AddAttribute(appStateSrc, "RegionId");
                    var disAttr = query.AddAttribute(appStateSrc, "DistrictId");
                    var djamAttr = query.AddAttribute(appStateSrc, "DjamoatId");
                    var dateAttr = query.AddAttribute(nonPaymentReportSrc, "LoadDate");
                    query.AddAttribute("&Id", SqlQuerySummaryFunction.Count);
                    var appAttr = query.AddAttribute(appSrc, "&Id");
                    query.AddGroupAttribute(regAttr);
                    query.AddGroupAttribute(disAttr);
                    query.AddGroupAttribute(djamAttr);
                    query.AddGroupAttribute(dateAttr);
                    query.AddGroupAttribute(appAttr);
                    var appList = new List<Guid>();

                    using (var reader = new SqlQueryReader(context.DataContext, query))
                    {
                        while (reader.Read())
                        {
                            var appId = reader.GetGuid(5);
                            if (appList.Contains(appId))
                                continue;
                            else
                                appList.Add(appId);
                            var regionId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                            var districtId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                            var djamoatId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                            if (regionId == Guid.Empty || districtId == Guid.Empty || djamoatId == Guid.Empty)
                                continue;
                            var loadDate = reader.GetDateTime(3);
                            var count = reader.IsDbNull(4) ? 0 : reader.GetInt32(4);

                            var item = GetItem(items, regionId, districtId, djamoatId);
                            if (GetQuarter(loadDate.Month) == 1)
                                item.NonPaidCount1 = ((int?)item.NonPaidCount1 ?? 0) + count;
                            else if (GetQuarter(loadDate.Month) == 2)
                                item.NonPaidCount2 = ((int?)item.NonPaidCount2 ?? 0) + count;
                            else if (GetQuarter(loadDate.Month) == 3)
                                item.NonPaidCount3 = ((int?)item.NonPaidCount3 ?? 0) + count;
                            else if (GetQuarter(loadDate.Month) == 4)
                                item.NonPaidCount4 = ((int?)item.NonPaidCount4 ?? 0) + count;
                        }
                    }
                }
                var AppCount = 0;
                foreach (var regions in items.GroupBy(x => x.RegionId))
                {
                    var regionItem = new BankingReportRegion();
                    regionItem.DateFormation = date;
                    regionItem.area = context.Documents.LoadById(regions.Key)["Name"].ToString();
                    regionItem.Year = date.Year;
                    var regAss1 = 0;
                    var regAss2 = 0;
                    var regAss3 = 0;
                    var regAss4 = 0;

                    var regPaid1 = 0;
                    var regPaid2 = 0;
                    var regPaid3 = 0;
                    var regPaid4 = 0;

                    var regNon1 = 0;
                    var regNon2 = 0;
                    var regNon3 = 0;
                    var regNon4 = 0;

                    foreach (var districts in regions.GroupBy(x => x.DistrictId))
                    {
                        var districtItem = new BankingReportDistrict();
                        districtItem.DateFormation = date;
                        districtItem.district = context.Documents.LoadById(districts.Key)["Name"].ToString();
                        districtItem.Year = date.Year;

                        var disAss1 = 0;
                        var disAss2 = 0;
                        var disAss3 = 0;
                        var disAss4 = 0;

                        var disPaid1 = 0;
                        var disPaid2 = 0;
                        var disPaid3 = 0;
                        var disPaid4 = 0;

                        var disNon1 = 0;
                        var disNon2 = 0;
                        var disNon3 = 0;
                        var disNon4 = 0;

                        foreach (var item in districts)
                        {
                            var djamoatItem = new BankingReportDjamoat();
                            djamoatItem.DateFormation = date;
                            djamoatItem.djamoat = context.Documents.LoadById(item.DjamoatId)["Name"].ToString();
                            djamoatItem.Year = date.Year;
                            var ass1 = item.AssignedCount1 ?? 0;
                            var ass2 = item.AssignedCount2 ?? 0;
                            var ass3 = item.AssignedCount3 ?? 0;
                            var ass4 = item.AssignedCount4 ?? 0;

                            var pay1 = item.PaidCount1 ?? 0;
                            var pay2 = item.PaidCount2 ?? 0;
                            var pay3 = item.PaidCount3 ?? 0;
                            var pay4 = item.PaidCount4 ?? 0;

                            var non1 = item.NonPaidCount1 ?? 0;
                            var non2 = item.NonPaidCount2 ?? 0;
                            var non3 = item.NonPaidCount3 ?? 0;
                            var non4 = item.NonPaidCount4 ?? 0;

                            djamoatItem.DjomAssignedQuart1 = ass1;
                            djamoatItem.DjomAssignedQuart2 = ass2;
                            djamoatItem.DjomAssignedQuart3 = ass3;
                            djamoatItem.DjomAssignedQuart4 = ass4;

                            djamoatItem.DjomPaidQuart1 = pay1;
                            djamoatItem.DjomPaidQuart2 = pay2;
                            djamoatItem.DjomPaidQuart3 = pay3;
                            djamoatItem.DjomPaidQuart4 = pay4;

                            djamoatItem.DjomNReceivedQuart1 = non1;
                            djamoatItem.DjomNReceivedQuart2 = non2;
                            djamoatItem.DjomNReceivedQuart3 = non3;
                            djamoatItem.DjomNReceivedQuart4 = non4;

                            if (ass1 > 0) djamoatItem.DjomPercentQuart1 = non1 * 100 / ass1;
                            if (ass2 > 0) djamoatItem.DjomPercentQuart2 = non2 * 100 / ass2;
                            if (ass3 > 0) djamoatItem.DjomPercentQuart3 = non3 * 100 / ass3;
                            if (ass4 > 0) djamoatItem.DjomPercentQuart4 = non4 * 100 / ass4;

                            districtItem.bankingReportDjamoats.Add(djamoatItem);

                            disAss1 += ass1;
                            disAss2 += ass2;
                            disAss3 += ass3;
                            disAss4 += ass4;

                            disPaid1 += pay1;
                            disPaid2 += pay2;
                            disPaid3 += pay3;
                            disPaid4 += pay4;

                            disNon1 += non1;
                            disNon2 += non2;
                            disNon3 += non3;
                            disNon4 += non4;
                        }

                        districtItem.DisAssignedQuart1 = disAss1;
                        districtItem.DisAssignedQuart2 = disAss1;
                        districtItem.DisAssignedQuart3 = disAss1;
                        districtItem.DisAssignedQuart4 = disAss1;


                        districtItem.DisPaidQuart1 = disPaid1;
                        districtItem.DisPaidQuart2 = disPaid2;
                        districtItem.DisPaidQuart3 = disPaid3;
                        districtItem.DisPaidQuart4 = disPaid4;

                        districtItem.DisNReceivedQuart1 = disNon1;
                        districtItem.DisNReceivedQuart2 = disNon2;
                        districtItem.DisNReceivedQuart3 = disNon3;
                        districtItem.DisNReceivedQuart4 = disNon4;

                        if (disAss1 > 0) districtItem.DisPercentQuart1 = disNon1 * 100 / disAss1;
                        if (disAss2 > 0) districtItem.DisPercentQuart2 = disNon2 * 100 / disAss2;
                        if (disAss3 > 0) districtItem.DisPercentQuart3 = disNon3 * 100 / disAss3;
                        if (disAss4 > 0) districtItem.DisPercentQuart4 = disNon4 * 100 / disAss4;

                        regionItem.bankingReportDistricts.Add(districtItem);

                        regAss1 += disAss1;
                        regAss2 += disAss2;
                        regAss3 += disAss3;
                        regAss4 += disAss4;

                        regPaid1 += disPaid1;
                        regPaid2 += disPaid2;
                        regPaid3 += disPaid3;
                        regPaid4 += disPaid4;

                        regNon1 += disNon1;
                        regNon2 += disNon2;
                        regNon3 += disNon3;
                        regNon4 += disNon4;
                    }

                    regionItem.AssignedQuart1 = regAss1;
                    regionItem.AssignedQuart2 = regAss2;
                    regionItem.AssignedQuart3 = regAss3;
                    regionItem.AssignedQuart4 = regAss4;

                    regionItem.PaidQuart1 = regPaid1;
                    regionItem.PaidQuart2 = regPaid2;
                    regionItem.PaidQuart3 = regPaid3;
                    regionItem.PaidQuart4 = regPaid4;

                    regionItem.NReceivedQuart1 = regNon1;
                    regionItem.NReceivedQuart2 = regNon2;
                    regionItem.NReceivedQuart3 = regNon3;
                    regionItem.NReceivedQuart4 = regNon4;

                    if (regAss1 > 0) regionItem.PercentQuart1 = regNon1 * 100 / regAss1;
                    if (regAss2 > 0) regionItem.PercentQuart2 = regNon2 * 100 / regAss2;
                    if (regAss3 > 0) regionItem.PercentQuart3 = regNon3 * 100 / regAss3;
                    if (regAss4 > 0) regionItem.PercentQuart4 = regNon4 * 100 / regAss4;

                    AppCount++;
                    report.bankingReportRegions.Add(regionItem);
                }

                report.AppCount = AppCount;
                report.DateFormation = date;
                report.Year = date.Year;
                report.Month = date.Month;
                return report;
            }

            private static readonly Guid reportDefId = new Guid("{B49E1FFC-75EA-4AF7-B3DB-911B3262A5F9}");
            private static readonly Guid reportItemRegionDefId = new Guid("{841BD68A-70D0-469C-9D71-1C2E3EF935DA}"); //по областям
            private static readonly Guid reportItemDistrictDefId = new Guid("{6CE15436-46F5-478F-B553-69014BD8827B}"); //по районам
            private static readonly Guid reportItemDjamoatDefId = new Guid("{E11DBD86-C9F6-4169-9C48-779559F6C1BC}"); //по джамоатам

            private static readonly Guid BankAccTernDefId = new Guid("{982C14E1-3F9C-4559-8835-314C612AB021}"); //Оборот банковского счета
            private static readonly Guid bankTurnowerRepoDefId = new Guid("{BF01AAF9-4838-42C9-8F47-0171DCCD9C3D}");//отчет об оборотах
            private static readonly Guid AppDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}");
            private static readonly Guid BankAccountDefId = new Guid("{BE6D5C1F-48A6-483B-980A-14CEFF781FD4}"); //Банковский счет
            private static readonly Guid AppStateDefId = new Guid("{547BBA55-2281-4388-A1FC-EE890168AC2D}");
            private static readonly Guid AssignmentDefId = new Guid("{51935CC6-CC48-4DAC-8853-DA8F57C057E8}");
            private static readonly Guid NonPaymentDefId = new Guid("{0D951982-B6E2-4BA6-B4E6-865295498E36}");
            private static readonly Guid NonPaymentReportDefId = new Guid("{BE079DC3-F704-4DCF-9C89-D2592CAA4A37}");

            private static readonly Guid OnPaymentStateId = new Guid("{78C294B5-B6EA-4075-9EEF-52073A6A2511}");
            private static readonly Guid AssignedAppStateId = new Guid("{ACB44CC8-BF44-44F4-8056-723CED22536C}");

            private static ReportItem GetItem(List<ReportItem> items, Guid regionId, Guid districtId, Guid djamoatId)
            {
                var item = items.FirstOrDefault(x => x.RegionId == regionId && x.DistrictId == districtId && x.DjamoatId == djamoatId);
                if (item != null) return item;
                var newItem = new ReportItem { RegionId = regionId, DistrictId = districtId, DjamoatId = djamoatId };
                items.Add(newItem);
                return newItem;
            }

            private static int GetQuarter(int m)
            {
                if (m <= 3) return 1;
                else if (m > 3 && m <= 6) return 2;
                else if (m > 6 && m <= 9) return 3;
                else if (m > 9 && m <= 12) return 4;
                throw new ApplicationException("Квартал не определен!");
            }

            private static DateTime FDByQuarter(int q, int y)
            {
                if (q == 1) return new DateTime(y, 1, 1);
                else if (q == 2) return new DateTime(y, 4, 1);
                else if (q == 3) return new DateTime(y, 7, 1);
                else if (q == 4) return new DateTime(y, 10, 1);
                throw new ApplicationException("Дата начала не определена!");
            }

            private static DateTime LDByQuarter(int q, int y)
            {
                if (q == 1) return new DateTime(y, 1, DateTime.DaysInMonth(y, 1));
                else if (q == 2) return new DateTime(y, 4, DateTime.DaysInMonth(y, 4));
                else if (q == 3) return new DateTime(y, 7, DateTime.DaysInMonth(y, 7));
                else if (q == 4) return new DateTime(y, 10, DateTime.DaysInMonth(y, 10));
                throw new ApplicationException("Дата начала не определена!");
            }

            private class ReportItem
            {
                public Guid RegionId;
                public Guid DistrictId;
                public Guid DjamoatId;
                public int? AssignedCount1;
                public int? AssignedCount2;
                public int? AssignedCount3;
                public int? AssignedCount4;
                public int? PaidCount1;
                public int? PaidCount2;
                public int? PaidCount3;
                public int? PaidCount4;
                public int? NonPaidCount1;
                public int? NonPaidCount2;
                public int? NonPaidCount3;
                public int? NonPaidCount4;
            }

            [DataContract]
            public class BankingReport
            {
                [DataMember]
                public DateTime DateFormation { get; set; }
                [DataMember]
                public int Year { get; set; }
                [DataMember]
                public int Month { get; set; }
                [DataMember]
                public string Note { get; set; }
                [DataMember]
                public List<BankingReportRegion> bankingReportRegions { get; set; }
                [DataMember]
                public int AppCount { get; set; }
                public BankingReport()
                {
                    bankingReportRegions = new List<BankingReportRegion>();
                }
            }

            [DataContract]
            public class BankingReportRegion
            {
                [DataMember]
                public int Year { get; set; }
                [DataMember]
                public DateTime DateFormation { get; set; }
                [DataMember]
                public int Month { get; set; }
                [DataMember]
                public string area { get; set; }
                [DataMember]
                public List<BankingReportDistrict> bankingReportDistricts { get; set; }
                [DataMember]
                public int Number { get; set; }
                [DataMember]
                public decimal PercentQuart1 { get; set; }
                [DataMember]
                public decimal PercentQuart2 { get; set; }
                [DataMember]
                public decimal PercentQuart3 { get; set; }
                [DataMember]
                public decimal PercentQuart4 { get; set; }
                [DataMember]
                public int AssignedQuart1 { get; set; }
                [DataMember]
                public int AssignedQuart2 { get; set; }
                [DataMember]
                public int AssignedQuart3 { get; set; }
                [DataMember]
                public int AssignedQuart4 { get; set; }
                [DataMember]
                public int NReceivedQuart1 { get; set; }
                [DataMember]
                public int NReceivedQuart2 { get; set; }
                [DataMember]
                public int NReceivedQuart3 { get; set; }
                [DataMember]
                public int NReceivedQuart4 { get; set; }
                [DataMember]
                public int PaidQuart1 { get; set; }
                [DataMember]
                public int PaidQuart2 { get; set; }
                [DataMember]
                public int PaidQuart3 { get; set; }
                [DataMember]
                public int PaidQuart4 { get; set; }
                public BankingReportRegion()
                {
                    bankingReportDistricts = new List<BankingReportDistrict>();
                }
            }

            [DataContract]
            public class BankingReportDistrict
            {
                [DataMember]
                public int countHousehold { get; set; }
                [DataMember]
                public List<BankingReportDjamoat> bankingReportDjamoats { get; set; }
                [DataMember]
                public int Year { get; set; }
                [DataMember]
                public DateTime DateFormation { get; set; }
                [DataMember]
                public string district { get; set; }
                [DataMember]
                public int DisNReceivedQuart1 { get; set; }
                [DataMember]
                public int DisNReceivedQuart2 { get; set; }
                [DataMember]
                public int DisNReceivedQuart3 { get; set; }
                [DataMember]
                public int DisNReceivedQuart4 { get; set; }
                [DataMember]
                public int DisPaidQuart1 { get; set; }
                [DataMember]
                public int DisPaidQuart2 { get; set; }
                [DataMember]
                public int DisPaidQuart3 { get; set; }
                [DataMember]
                public int DisPaidQuart4 { get; set; }
                [DataMember]
                public int DisAssignedQuart1 { get; set; }
                [DataMember]
                public int DisAssignedQuart2 { get; set; }
                [DataMember]
                public int DisAssignedQuart3 { get; set; }
                [DataMember]
                public int DisAssignedQuart4 { get; set; }
                [DataMember]
                public decimal DisPercentQuart1 { get; set; }
                [DataMember]
                public decimal DisPercentQuart2 { get; set; }
                [DataMember]
                public decimal DisPercentQuart3 { get; set; }
                [DataMember]
                public decimal DisPercentQuart4 { get; set; }
                public BankingReportDistrict()
                {
                    bankingReportDjamoats = new List<BankingReportDjamoat>();
                }
            }
            [DataContract]
            public class BankingReportDjamoat
            {
                [DataMember]
                public int count { get; set; }
                [DataMember]
                public int Year { get; set; }
                [DataMember]
                public DateTime DateFormation { get; set; }
                [DataMember]
                public string djamoat { get; set; }
                [DataMember]
                public int DjomNReceivedQuart1 { get; set; }
                [DataMember]
                public int DjomNReceivedQuart2 { get; set; }
                [DataMember]
                public int DjomNReceivedQuart3 { get; set; }
                [DataMember]
                public int DjomNReceivedQuart4 { get; set; }
                [DataMember]
                public int DjomPaidQuart1 { get; set; }
                [DataMember]
                public int DjomPaidQuart2 { get; set; }
                [DataMember]
                public int DjomPaidQuart3 { get; set; }
                [DataMember]
                public int DjomPaidQuart4 { get; set; }
                [DataMember]
                public int DjomAssignedQuart1 { get; set; }
                [DataMember]
                public int DjomAssignedQuart2 { get; set; }
                [DataMember]
                public int DjomAssignedQuart3 { get; set; }
                [DataMember]
                public int DjomAssignedQuart4 { get; set; }
                [DataMember]
                public decimal DjomPercentQuart1 { get; set; }
                [DataMember]
                public decimal DjomPercentQuart2 { get; set; }
                [DataMember]
                public decimal DjomPercentQuart3 { get; set; }
                [DataMember]
                public decimal DjomPercentQuart4 { get; set; }

            }
        }
        //Поименный список получателей пособий
        public static class Report_10g
        {
            public static List<ReportItem> Execute(/*DateTime fd, DateTime ld*/)
            {
                var items = new List<ReportItem>();
                var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));
                CalcItems(context, /*fd, ld, */items);

                return items;
            }
            public static void CalcItems(WorkflowContext context,/* DateTime fd, DateTime ld,*/ List<ReportItem> items)
            {
                var qb = new QueryBuilder(paymentDefId);
                qb.Where("BankAccount").Include("Application").Include/*("Date").Ge(fd).And("Date").Le(ld).And*/("&State").Eq(new Guid("{78C294B5-B6EA-4075-9EEF-52073A6A2511}")).End().End();

                using (var query = SqlQueryBuilder.Build(context.DataContext, qb.Def))
                {
                    var bankAccountSrc = query.JoinSource(query.Source, bankAccountDefId, SqlSourceJoinType.Inner, "BankAccount");
                    var appSrc = query.JoinSource(bankAccountSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                    var appStateSrc = query.JoinSource(appSrc, appSatetDefId, SqlSourceJoinType.Inner, "Application_State");
                    var personSrc = query.JoinSource(appSrc, personDefId, SqlSourceJoinType.Inner, "Person");
                    var assignSrc = query.JoinSource(query.Source, assignDefId, SqlSourceJoinType.Inner, "Assignment");
                    var areaSrc = query.JoinSource(appStateSrc, regionDefId, SqlSourceJoinType.Inner, "RegionId");
                    var districtSrc = query.JoinSource(appStateSrc, districtDefId, SqlSourceJoinType.Inner, "DistrictId");
                    var djamoatSrc = query.JoinSource(appStateSrc, djamoatDefId, SqlSourceJoinType.LeftOuter, "DjamoatId");

                    query.AddAttribute(query.Source, "&Id");
                    query.AddAttribute(areaSrc, "Name");
                    query.AddAttribute(districtSrc, "Name");
                    query.AddAttribute(djamoatSrc, "Name");
                    query.AddAttribute(personSrc, "Last_Name");
                    query.AddAttribute(personSrc, "First_Name");
                    query.AddAttribute(personSrc, "Middle_Name");
                    query.AddAttribute(appSrc, "No");
                    query.AddAttribute(appSrc, "Date");
                    query.AddAttribute(assignSrc, "Amount");
                    query.AddAttribute(appSrc, "&Id");
                    query.AddAttribute(personSrc, "&Id");
                    query.AddAttribute(assignSrc, "No");

                    var table = new DataTable();
                    using (var reader = new SqlQueryReader(query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    int i = 0;
                    foreach (DataRow row in table.Rows)
                    {
                        var paymentId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                        var regionName = row[1] is DBNull ? "" : row[1].ToString();
                        var districtName = row[2] is DBNull ? "" : row[2].ToString();
                        var djamoatName = row[3] is DBNull ? "" : row[3].ToString();
                        var lstName = row[4] is DBNull ? "" : row[4].ToString();
                        var fstName = row[5] is DBNull ? "" : row[5].ToString();
                        var mdlName = row[6] is DBNull ? "" : row[6].ToString();
                        var regNo = row[7] is DBNull ? "" : row[7].ToString();
                        var regDate = row[8] is DBNull ? DateTime.MaxValue : (DateTime)row[8];
                        var amount = row[9] is DBNull ? 0m : (decimal)row[9];
                        var appId = row[10] is DBNull ? Guid.Empty : (Guid)row[10];
                        var personId = row[11] is DBNull ? Guid.Empty : (Guid)row[11];
                        var assignNo = row[12] is DBNull ? 0 : (int)row[12];

                        var item = new ReportItem();
                        i++;
                        //if (paymentId != Guid.Empty) continue;
                        if (appId != Guid.Empty)
                        {
                            if (!string.IsNullOrEmpty(regionName))
                            {
                                item.gr1 += regionName;
                                if (!string.IsNullOrEmpty(districtName))
                                {
                                    item.gr2 += districtName;
                                }
                                if (!string.IsNullOrEmpty(djamoatName))
                                {
                                    item.gr3 += djamoatName;
                                }
                            }
                            if (!string.IsNullOrEmpty(regNo))
                                item.gr4 += regNo;
                                item.gr5 = regDate;

                            if (personId != Guid.Empty)
                            {
                                if (!string.IsNullOrEmpty(lstName))
                                    item.gr6 += lstName;
                                if (!string.IsNullOrEmpty(fstName))
                                    item.gr7 += fstName;
                                if (!string.IsNullOrEmpty(mdlName))
                                    item.gr8 += mdlName;
                            }
                            if (amount != 0m)
                            {
                                var monthAmount = amount * assignNo;
                                item.gr9 += monthAmount;
                            }
                            item.no = i;
                            items.Add(item);
                        }                        
                    }
                }
            }
            private static readonly Guid paymentDefId = new Guid("{68667FBB-C149-4FB3-93AD-1BBCE3936B6E}"); //Оплата
            private static readonly Guid bankAccountDefId = new Guid("{BE6D5C1F-48A6-483B-980A-14CEFF781FD4}"); //Банковский счет
            private static readonly Guid appDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}"); //заявления
            private static readonly Guid assignDefId = new Guid("{51935CC6-CC48-4DAC-8853-DA8F57C057E8}");  //Назначение
            private static readonly Guid appSatetDefId = new Guid("{547BBA55-2281-4388-A1FC-EE890168AC2D}"); //Анкета домохозяйства
            private static readonly Guid personDefId = new Guid("{6052978A-1ECB-4F96-A16B-93548936AFC0}"); //Гражданин
            private static readonly Guid regionDefId = new Guid("{8C5E9217-59AC-4B4E-A41A-643FC34444E4}"); //Область
            private static readonly Guid districtDefId = new Guid("{4D029337-C025-442E-8E93-AFD1852073AC}"); //Район
            private static readonly Guid djamoatDefId = new Guid("{967D525D-9B76-44BE-93FA-BD4639EA515A}"); //Джамоат

            public class ReportItem
            {
                public int no { get; set; }
                public string gr1 { get; set; }
                public string gr2 { get; set; }
                public string gr3 { get; set; }
                public string gr4 { get; set; }
                public DateTime gr5 { get; set; }
                public string gr6 { get; set; }
                public string gr7 { get; set; }
                public string gr8 { get; set; }
                public string gr9 { get; set; }
            }
        }

        //Информаця о получателях АСП
        public static class Report_10v
        {
            public static readonly Guid UserId = new Guid("{E0F19306-AECE-477B-B110-3AD09323DD2D}");
            public static readonly string Username = "Admin";
            public static readonly Guid regionId = new Guid("{8c5e9217-59ac-4b4e-a41a-643fc34444e4}");
            public static readonly Guid districtId = new Guid("{4D029337-C025-442E-8E93-AFD1852073AC}");
            public static readonly Guid djamoatId = new Guid("{967D525D-9B76-44BE-93FA-BD4639EA515A}");
            public static readonly Guid AppDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}"); //Заявление на АСП
            public static readonly Guid AppStateDefId = new Guid("{547BBA55-2281-4388-A1FC-EE890168AC2D}");

            public static readonly Guid AssignedStateId = new Guid("{ACB44CC8-BF44-44F4-8056-723CED22536C}");
            public static readonly Guid OnPaymentStateId = new Guid("{78C294B5-B6EA-4075-9EEF-52073A6A2511}");
            public static readonly Guid RefusedStateId = new Guid("{5D8FF804-E287-41D5-8594-35A333F3CB49}"); //Отказано  
            public class Place
            {
                public Guid Id { get; set; }
                public string Name { get; set; }
                public Guid Def { get; set; }
                public int? quantityAllFromBeginOfYear { get; set; }
                public int? quantityAssignedFromBeginOfYear { get; set; }
                public int? quantityRefusedFromBeginOfYear { get; set; }
                public int? quantityOnPaymentFromBeginOfYear { get; set; }
            }

            [DataContract]
            public class RegionObject
            {
                [DataMember]
                public Guid Id { get; set; }
                [DataMember]
                public string Name { get; set; }
                [DataMember]
                public Guid Def { get; set; }
                [DataMember]
                public int? quantityAllFromBeginOfYear { get; set; }
                [DataMember]
                public int? quantityAssignedFromBeginOfYear { get; set; }
                [DataMember]
                public int? quantityRefusedFromBeginOfYear { get; set; }
                [DataMember]
                public int? quantityOnPaymentFromBeginOfYear { get; set; }
                [DataMember]
                public List<DistrictObject> districts { get; set; }
                public RegionObject()
                {
                    districts = new List<DistrictObject>();
                }
            }

            [DataContract]
            public class DistrictObject
            {
                [DataMember]
                public Guid Id { get; set; }
                [DataMember]
                public string Name { get; set; }
                [DataMember]
                public string RegionName { get; set; }
                [DataMember]
                public Guid Def { get; set; }
                [DataMember]
                public int? quantityAllFromBeginOfYear { get; set; }
                [DataMember]
                public int? quantityAssignedFromBeginOfYear { get; set; }
                [DataMember]
                public int? quantityRefusedFromBeginOfYear { get; set; }
                [DataMember]
                public int? quantityOnPaymentFromBeginOfYear { get; set; }
                [DataMember]
                public List<DjamoatObject> djamoats { get; set; }
                public DistrictObject()
                {
                    djamoats = new List<DjamoatObject>();
                }

            }

            [DataContract]
            public class DjamoatObject
            {
                [DataMember]
                public Guid Id { get; set; }
                [DataMember]
                public string Name { get; set; }
                [DataMember]
                public string DistrictName { get; set; }
                [DataMember]
                public Guid Def { get; set; }
                [DataMember]
                public int? quantityAllFromBeginOfYear { get; set; }
                [DataMember]
                public int? quantityAssignedFromBeginOfYear { get; set; }
                [DataMember]
                public int? quantityRefusedFromBeginOfYear { get; set; }
                [DataMember]
                public int? quantityOnPaymentFromBeginOfYear { get; set; }
            }

            public static List<Place> GetListFromSqlQueryReader(WorkflowContext context, Guid PlaceId, string columnName)
            {
                List<Place> placeList = new List<Place>();
                var docRepo = context.Documents;
                var docDefRepo = context.DocDefs;
                var docDef = docDefRepo.DocDefById(PlaceId);
                var qb = new QueryBuilder(PlaceId, context.UserId);
                var sqlQueryBuilder = context.Get<ISqlQueryBuilder>();
                using (var query = sqlQueryBuilder.Build(qb.Def))
                {
                    var q = query.BuildSql().ToString();
                    docDef.Attributes.ForEach(x =>
                    {
                        query.AddAttribute(x.Name);
                    });
                    var table = new DataTable();
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    foreach (DataRow row in table.Rows)
                    {
                        Place place = new Place();
                        place.Id = Guid.Parse(row["Id1"].ToString());
                        place.Name = row["Name"].ToString();
                        place.Def = Guid.Parse(row[columnName].ToString());
                        placeList.Add(place);
                    }
                }
                return placeList;
            }

            public static List<RegionObject> GetAdministrativeDivision(List<ReportItem> reportItems)
            {
                List<RegionObject> regionList = new List<RegionObject>();
                List<DjamoatObject> djamoatList = new List<DjamoatObject>();
                List<DistrictObject> districtList = new List<DistrictObject>();
                var context = CreateAsistContext(Username, UserId);
                var areaList = GetListFromSqlQueryReader(context, regionId, "Id1");

                var _districtList = GetListFromSqlQueryReader(context, districtId, "Area");
                var _djamoatList = GetListFromSqlQueryReader(context, djamoatId, "District");
                foreach (var djamoat in _djamoatList)
                {

                    DjamoatObject _djamoat = new DjamoatObject();
                    _djamoat.Id = djamoat.Id;
                    _djamoat.Name = djamoat.Name;
                    _djamoat.Def = djamoat.Def;
                    _djamoat.DistrictName = context.Documents.LoadById(djamoat.Def)["Name"].ToString();
                    _djamoat.quantityAllFromBeginOfYear = reportItems.Where(x => x.DjamoatId.Equals(djamoat.Id)).ToList().Count;
                    _djamoat.quantityAssignedFromBeginOfYear = reportItems.Where(x => x.DjamoatId.Equals(djamoat.Id) && x.StateId.Equals(AssignedStateId)).ToList().Count;
                    _djamoat.quantityOnPaymentFromBeginOfYear = reportItems.Where(x => x.DjamoatId.Equals(djamoat.Id) && x.StateId.Equals(OnPaymentStateId)).ToList().Count;
                    var qqq = reportItems.Where(x => x.DjamoatId.Equals(djamoat.Id) && x.StateId.Equals(OnPaymentStateId)).ToList();
                    _djamoat.quantityRefusedFromBeginOfYear = reportItems.Where(x => x.DjamoatId.Equals(djamoat.Id)).Where(x => x.StateId.Equals(RefusedStateId)).ToList().Count;
                    djamoatList.Add(_djamoat);

                }
                foreach (var district in _districtList)
                {
                    DistrictObject _district = new DistrictObject();
                    _district.Id = district.Id;
                    _district.Name = district.Name;
                    _district.Def = district.Def;
                    _district.RegionName = context.Documents.LoadById(district.Def)["Name"].ToString();
                    _district.quantityAllFromBeginOfYear = reportItems.Where(x => x.DistrictId.Equals(district.Id)).Count();
                    _district.quantityAssignedFromBeginOfYear = reportItems.Where(x => (x.DistrictId.Equals(district.Id)) && (x.StateId.Equals(AssignedStateId))).Count();
                    _district.quantityOnPaymentFromBeginOfYear = reportItems.Where(x => (x.DistrictId.Equals(district.Id)) && (x.StateId.Equals(OnPaymentStateId))).Count();
                    _district.quantityRefusedFromBeginOfYear = reportItems.Where(x => (x.DistrictId.Equals(district.Id)) && (x.StateId.Equals(RefusedStateId))).Count();
                    _district.djamoats = djamoatList.Where(x => x.Def.Equals(district.Id)).ToList();
                    districtList.Add(_district);
                }
                foreach (var region in areaList)
                {
                    regionList.Add
                                    (new RegionObject
                                    {
                                        Id = region.Id,
                                        Name = region.Name,
                                        Def = region.Def,
                                        quantityAllFromBeginOfYear = reportItems.Where(x => x.RegionId.Equals(region.Id)).Count(),
                                        quantityAssignedFromBeginOfYear = reportItems.Where(x => (x.RegionId.Equals(region.Id)) && (x.StateId.Equals(AssignedStateId))).Count(),
                                        quantityOnPaymentFromBeginOfYear = reportItems.Where(x => (x.RegionId.Equals(region.Id)) && (x.StateId.Equals(OnPaymentStateId))).Count(),
                                        quantityRefusedFromBeginOfYear = reportItems.Where(x => (x.RegionId.Equals(region.Id)) && (x.StateId.Equals(RefusedStateId))).Count(),
                                        districts = districtList.Where(x => x.Def.Equals(region.Id)).ToList()
                                    }
                                    );
                }
                return regionList;
            }
            public static string Execute(DateTime fd, DateTime ld)
            {
                var reportItems = GetReportItems(fd, ld);
                var regionList = GetAdministrativeDivision(reportItems);
                return JsonConvert.SerializeObject(regionList);
            }
            public static List<ReportItem> GetReportItems(DateTime fd, DateTime ld)
            {
                var context = CreateAsistContext("sod_user", new Guid("{4296EF4D-ED7A-41F8-BE88-2684AD21AC0E}"));
                var docRepo = context.Documents;

                if (fd == DateTime.MinValue || ld == DateTime.MaxValue || ld <= fd)
                    throw new ApplicationException("Ошибка в периоде!");

                var items = new List<ReportItem>();
                var qb = new QueryBuilder(AppDefId);

                using (var query = context.CreateSqlQuery(qb.Def))
                {
                    var appStateSource = query.JoinSource(query.Source, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");
                    query.AndCondition(query.Source, "Date", ConditionOperation.GreatEqual, fd);
                    query.AndCondition(query.Source, "Date", ConditionOperation.LessThen, ld.AddDays(1));

                    query.AddAttribute(appStateSource, "DistrictId");
                    query.AddAttribute(appStateSource, "DjamoatId");
                    query.AddAttribute(query.Source, "&State");
                    query.AddAttribute(appStateSource, "All");
                    query.AddAttribute(query.Source, "&Id");
                    query.AddAttribute(appStateSource, "RegionId");

                    using (var reader = context.CreateSqlReader(query))
                    {
                        while (reader.Read())
                        {
                            var district_Id = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                            var djamoat_Id = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                            var stateId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                            var allFamilyCount = reader.IsDbNull(3) ? 0 : reader.GetInt32(3);
                            var regionId = reader.IsDbNull(5) ? Guid.Empty : reader.GetGuid(5);
                            if (djamoat_Id == Guid.Empty && district_Id == Guid.Empty && stateId == Guid.Empty) continue;
                            var item = new ReportItem { RegionId = regionId, DistrictId = district_Id, DjamoatId = djamoat_Id, StateId = stateId };
                            items.Add(item);
                        }
                    }
                }
                return items;
            }

            private static ReportItem GetItem(List<ReportItem> items, Guid regionId, Guid districtId, Guid djamoatId, Guid stateId)
            {
                var item = items.FirstOrDefault(x => x.DistrictId == districtId && x.DjamoatId == djamoatId && x.RegionId == regionId);
                if (item != null)
                    return item;
                var newItem = new ReportItem
                {
                    RegionId = regionId,
                    DistrictId = districtId,
                    DjamoatId = djamoatId,
                    StateId = stateId
                };
                items.Add(newItem);
                return newItem;
            }

            public class ReportItem
            {
                public Guid RegionId;
                public Guid DistrictId;
                public Guid DjamoatId;
                public Guid StateId;
            }
        }
        //отчет по оператором
        public static class GetStatisticDataInput
        {
            public static readonly Guid UserId = new Guid("{E0F19306-AECE-477B-B110-3AD09323DD2D}");
            public static readonly string Username = "Admin";
            public static readonly Guid regionId = new Guid("{8c5e9217-59ac-4b4e-a41a-643fc34444e4}");
            public static readonly Guid districtId = new Guid("{4D029337-C025-442E-8E93-AFD1852073AC}");
            public static readonly Guid AppDefId = new Guid("{4F9F2AE2-7180-4850-A3F4-5FB47313BCC0}"); //Заявление на АСП
            public static readonly Guid sampleId = new Guid("{4cbdc11c-7e7b-4be0-b884-e2bcc124bf13}");
            public static readonly Guid AppStateDefId = new Guid("{547BBA55-2281-4388-A1FC-EE890168AC2D}");


            public static List<User> Execute(int year, int month)
            {
                var regionObjects = GetAdministrativeDivision(year, month);
                return regionObjects;
            }
            [DataContract]
            public class RegionObject
            {
                [DataMember]
                public Guid Id { get; set; }
                [DataMember]
                public string Name { get; set; }
                [DataMember]
                public Guid Def { get; set; }
                [DataMember]
                public List<DistrictObject> districts { get; set; }
                public RegionObject()
                {
                    districts = new List<DistrictObject>();
                }
            }

            [DataContract]
            public class DistrictObject
            {
                [DataMember]
                public Guid Id { get; set; }
                [DataMember]
                public string Name { get; set; }
                [DataMember]
                public string RegionName { get; set; }
                [DataMember]
                public Guid Def { get; set; }
                [DataMember]
                public List<User> Users { get; set; }

                public DistrictObject()
                {
                    Users = new List<User>();
                }

            }

            [DataContract]
            public class User
            {
                [DataMember]
                public Guid Id { get; set; }
                [DataMember]
                public Guid DistrictId { get; set; }

                [DataMember]
                public string DistrictName { get; set; }
                [DataMember]
                public string RegionName { get; set; }
                [DataMember]
                public string FullName { get; set; }
                [DataMember]
                public int[] QuantityDoc { get; set; }
                [DataMember]
                public int TotalDoc { get; set; }

                public User(int daysInMonth)
                {
                    QuantityDoc = new int[daysInMonth];
                }
            }

            public class Place
            {
                public Guid Id { get; set; }
                public string Name { get; set; }
                public Guid Def { get; set; }
            }

            public static List<Place> GetListFromSqlQueryReader(WorkflowContext context, Guid PlaceId, string columnName)
            {
                List<Place> placeList = new List<Place>();
                var docRepo = context.Documents;
                var docDefRepo = context.DocDefs;
                var docDef = docDefRepo.DocDefById(PlaceId);
                var qb = new QueryBuilder(PlaceId, context.UserId);
                var sqlQueryBuilder = context.Get<ISqlQueryBuilder>();
                using (var query = sqlQueryBuilder.Build(qb.Def))
                {
                    var q = query.BuildSql().ToString();
                    docDef.Attributes.ForEach(x =>
                    {
                        query.AddAttribute(x.Name);
                    });
                    var table = new DataTable();
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    foreach (DataRow row in table.Rows)
                    {
                        Place place = new Place();
                        place.Id = Guid.Parse(row["Id1"].ToString());
                        place.Name = row["Name"].ToString();
                        place.Def = Guid.Parse(row[columnName].ToString());
                        placeList.Add(place);
                    }
                }
                return placeList;
            }

            public static List<User> GetAdministrativeDivision(int year, int month)
            {
                // var year = DateTime.Now.Year;
                // var month = DateTime.Now.Month;
                List<User> UserList = new List<User>();
                var daysInMonth = DateTime.DaysInMonth(year, month);
                var reportItems = GetReportItems(AppDefId, year, month);
                List<RegionObject> regionList = new List<RegionObject>();
                List<DistrictObject> districtList = new List<DistrictObject>();
                var context = CreateAsistContext(Username, UserId);
                var areaList = GetListFromSqlQueryReader(context, regionId, "Id1");
                var _districtList = GetListFromSqlQueryReader(context, districtId, "Area");
                foreach (var district in _districtList)
                {
                    DistrictObject _district = new DistrictObject();
                    _district.Id = district.Id;
                    _district.Name = district.Name;
                    _district.Def = district.Def;
                    _district.RegionName = context.Documents.LoadById(district.Def)["Name"].ToString();

                    foreach (var subItems in reportItems.Where(x => x.DistrictId.Equals(district.Id)).GroupBy(x => x.UserId))
                    {
                        int totalDoc = 0;
                        User user = new User(daysInMonth);
                        user.Id = subItems.Key;
                        var foundUser = context.Users.GetUserInfo(subItems.Key);
                        user.FullName = foundUser.FirstName + " " + foundUser.LastName;
                        user.DistrictId = _district.Id;
                        user.DistrictName = _district.Name;
                        user.RegionName = _district.RegionName;
                        for (var i = 0; i < daysInMonth; i++)
                        {
                            user.QuantityDoc[i] = subItems.Where(x => (x.DocumentDate.Day == i)).Count();
                            totalDoc += user.QuantityDoc[i];
                        }
                        user.TotalDoc = totalDoc;
                        _district.Users.Add(user);
                        UserList.Add(user);
                    }

                    districtList.Add(_district);
                }
                foreach (var region in areaList)
                {
                    regionList.Add
                                    (new RegionObject
                                    {
                                        Id = region.Id,
                                        Name = region.Name,
                                        Def = region.Def,
                                        districts = districtList.Where(x => x.Def.Equals(region.Id)).ToList(),
                                    }
                                    );
                }
                // return regionList;
                return UserList;
            }

            public static List<ReportItem> GetReportItems(Guid reportId, int year, int month)
            {
                // var year = DateTime.Now.Year;
                // var month = DateTime.Now.Month;
                var fd = new DateTime(year, month, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var reportItems = new List<ReportItem>();
                var context = CreateAsistContext(Username, UserId);
                var docRepo = context.Documents;
                var docDefRepo = context.DocDefs;
                var docDef = docDefRepo.DocDefById(reportId);
                var qb = new QueryBuilder(reportId, context.UserId);
                qb.Where("Date").Ge(fd).And("Date").Le(ld).End();

                using (var query = context.CreateSqlQuery(qb.Def))
                {
                    var appStateSource = query.JoinSource(query.Source, AppStateDefId, SqlSourceJoinType.Inner, "Application_State");
                    query.AddAttribute(query.Source, "&Id");
                    query.AddAttribute(appStateSource, "DistrictId");
                    query.AddAttribute(appStateSource, "RegionId");
                    query.AddAttribute(query.Source, "Date");
                    query.AddAttribute(query.Source, "&UserId");
                    using (var reader = context.CreateSqlReader(query))
                    {

                        while (reader.Read())
                        {
                            var docId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                            // var doc = docRepo.LoadById(docId);
                            var districtId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                            var regionId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                            var docDate = reader.IsDbNull(3) ? DateTime.MinValue : reader.GetDateTime(3);
                            var userId = reader.IsDbNull(3) ? Guid.Empty : reader.GetGuid(4);
                            if (districtId == Guid.Empty && regionId == Guid.Empty && docId == Guid.Empty) continue;
                            var item = new ReportItem { RegionId = regionId, DistrictId = districtId, UserId = userId, DocumentDate = docDate };
                            reportItems.Add(item);
                        }
                    }
                }
                return reportItems;
            }


            public class ReportItem
            {
                public Guid UserId { get; set; }
                public Guid DistrictId { get; set; }
                public Guid RegionId { get; set; }
                public DateTime DocumentDate { get; set; }
            }
        }
        public static class Converter
        {
            public static readonly Guid UserId = new Guid("{E0F19306-AECE-477B-B110-3AD09323DD2D}");
            public static readonly string Username = "Admin";
            public static string GetSQLString(Guid reportId, string json)
            {
                var context = CreateAsistContext(Username, UserId);
                var docRepo = context.Documents;
                var docDefRepo = context.DocDefs;
                var docDef = docDefRepo.DocDefById(reportId);
                var qb = new QueryBuilder(reportId, context.UserId);
                var sqlQueryBuilder = context.Get<ISqlQueryBuilder>();
                using (var query = sqlQueryBuilder.Build(qb.Def))
                {
                    var q = query.BuildSql().ToString();
                    docDef.Attributes.ForEach(x =>
                    {
                        query.AddAttribute(x.Name);
                        q = query.BuildSql().ToString();
                    });
                    return query.BuildSql().ToString();
                }
            }
        }
    }   
}




