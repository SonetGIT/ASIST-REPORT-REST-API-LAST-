using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Intersoft.CISSA.DataAccessLayer.Model.Workflow;
using Intersoft.CISSA.DataAccessLayer.Model.Query.Builders;
using Intersoft.CISSA.DataAccessLayer.Model.Query.Sql;
using System.IO;
using ASIST_REPORT_REST_API.Models;
using Domain.ResponseTypes;

namespace ASIST_REPORT_REST_API.Util
{
    public class AdoptedChildrenReport
    {
        static Guid adoptionOrgDefId = new Guid("{4F112044-51D0-4048-866A-EF6141893E1F}");
        static Guid childDefId = new Guid("{2CFF5093-F55F-4335-ADAA-EC951C41A770}");
        static Guid candidateDefId = new Guid("{35B15C63-13ED-4B17-A57A-539C4D38A985}");
        static Guid personDefId = new Guid("{D71CE61A-9B59-4B5E-8713-8131DBB5BA02}");
        static Guid childrenHouseDefId = new Guid("{C0C2A073-DE36-4B84-A370-81D264501760}");

        //stateTypes
        static Guid registeredStateTypeId = new Guid("{C1414D0C-417A-45AB-8B57-01D30A567F08}");
        static Guid expiredStateTypeId = new Guid("{5C47AE00-0409-4DE0-BFEB-877EB8F6AFCA}");
        static Guid removedStateTypeId = new Guid("{6556BEAF-CE93-4435-9C1E-7942F2E58316}");

        //EnumDefs
        static Guid genderEnumDefId = new Guid("{C780CE23-AC09-4CC3-8147-7779F6D80B65}");
        static Guid maleEnumItemId = new Guid("{74C6C7FE-53C6-4492-A62F-65A7A49AB644}");
        static Guid femaleEnumItemId = new Guid("{56E07640-5B5B-47FA-832D-A6639F36EB71}");


        static Guid unknownGenderEnumItemId = new Guid("{F36CFF9F-B072-43C3-BBDE-877D6D8D05B6}");
        static Guid unknownNationalityEnumItemId = new Guid("{E6642F79-D74A-4AE5-B868-F7724B912EA3}");

        static Guid nationalityEnumDefId = new Guid("{E8C21A14-0362-4279-9D6C-E41CAE54EB50}");


        public static AdoptedChildrenReportResponse Execute()
        {
            var context = ScriptExecutor.CreateAsistContext("ЧолпонЖумакунова", new Guid("{FCF9FB46-9617-4052-ACA3-1582594E8FC5}"));

            var enumRepo = context.Enums;

            var genderList = enumRepo.GetEnumItems(genderEnumDefId);
            var nationList = enumRepo.GetEnumItems(nationalityEnumDefId);
            var regions = new string[] { "Бишкек", "Чуйская", "Ошская", "Ош", "Баткен", "Ыссык", "Нарын", "Талас", "Джалал" };

            var genderCountList = new Dictionary<Guid, int>();
            var ageCountList = new Dictionary<int, Dictionary<Guid, int>>();
            var nationCountList = new Dictionary<Guid?, Dictionary<Guid, int>>();
            var placeCountList = new Dictionary<string, Dictionary<Guid, int>>();

            var childs = new List<ReportItem>();
            var expiredStateTypeId = new Guid("{5C47AE00-0409-4DE0-BFEB-877EB8F6AFCA}");
            var qb = new QueryBuilder(childDefId, context.UserId);
            qb.Where("&State").Neq(expiredStateTypeId);
            var query = context.CreateSqlQuery(qb.Def);
            var personSrc = query.JoinSource(query.Source, personDefId, SqlSourceJoinType.Inner, "Person");
            var childrenHouseSrc = query.JoinSource(query.Source, childrenHouseDefId, SqlSourceJoinType.Inner, "ChildrenHouse");
            query.AddAttribute(query.Source, "&Id");
            query.AddAttribute(personSrc, "Gender");
            query.AddAttribute(personSrc, "Nationality");
            query.AddAttribute(personSrc, "BirthDate");
            query.AddAttribute(childrenHouseSrc, "Name");
            using (var reader = context.CreateSqlReader(query))
            {
                while (reader.Read())
                {
                    childs.Add(new ReportItem
                    {
                        accountId = reader.GetGuid(0),
                        GenderId = reader.IsDbNull(1) ? unknownGenderEnumItemId : reader.GetGuid(1),
                        NationalityId = reader.IsDbNull(2) ? unknownNationalityEnumItemId : reader.GetGuid(2),
                        BirthDate = reader.IsDbNull(3) ? null : (DateTime?)reader.GetDateTime(3),
                        Address = reader.IsDbNull(4) ? "" : reader.GetString(4)
                    });
                }
            }
            regions = childs.Select(x => x.Address).Distinct().ToArray();
            foreach (var child in childs)
            {
                if (genderCountList.ContainsKey(child.GenderId))
                    genderCountList[child.GenderId]++;
                else
                    genderCountList.Add(child.GenderId, 1);

                var birthDate = child.BirthDate;
                int age = birthDate != null ? (DateTime.Today - (DateTime)birthDate).Days / 365 : 0;

                Dictionary<Guid, int> rec;
                if (!ageCountList.ContainsKey(age))
                {
                    rec = new Dictionary<Guid, int>();
                    rec.Add(maleEnumItemId, 0);
                    rec.Add(femaleEnumItemId, 0);
                    rec.Add(unknownGenderEnumItemId, 0);
                    ageCountList.Add(age, rec);
                }
                rec = ageCountList[age];
                rec[child.GenderId]++;

                var nation = child.NationalityId;

                if (!nationCountList.ContainsKey(nation))
                {
                    rec = new Dictionary<Guid, int>();
                    rec.Add(maleEnumItemId, 0);
                    rec.Add(femaleEnumItemId, 0);
                    rec.Add(unknownGenderEnumItemId, 0);
                    nationCountList.Add(nation, rec);
                }
                rec = nationCountList[nation];
                rec[child.GenderId]++;

                var address = child.Address;

                foreach (var region in regions)
                {
                    if (address.ToUpper() == region.ToUpper())
                    {
                        if (!placeCountList.ContainsKey(region))
                        {
                            rec = new Dictionary<Guid, int>();
                            rec.Add(maleEnumItemId, 0);
                            rec.Add(femaleEnumItemId, 0);
                            rec.Add(unknownGenderEnumItemId, 0);
                            placeCountList.Add(region, rec);
                        }

                        rec = placeCountList[region];
                        rec[child.GenderId]++;
                    }
                }
            }

            //const string title = "Отчетные данные по государственному банку данных о детях оставшихся без попечения родителей";
            var report = new AdoptedChildrenReportResponse();
            
            Dictionary<Guid, int> val;
            int r = 1;
            var byAges = new List<AdoptedChildrenReportItem>();
            foreach (var age in ageCountList.OrderBy(p => p.Key))
            {
                string name = new[] { 0, 1 }.Contains(age.Key) ? "год" : new[] { 2, 3, 4 }.Contains(age.Key) ? "года" : "лет";
                val = age.Value;
                var reportItem = new AdoptedChildrenReportItem
                {
                    Name = string.Format("{0} {1}", age.Key, name),
                    Boys = val[maleEnumItemId],
                    Girls = val[femaleEnumItemId],
                    Total = val[maleEnumItemId] + val[femaleEnumItemId] + val[unknownGenderEnumItemId],
                    No = r
                };
                r++;
                byAges.Add(reportItem);
            }
            
            r = 1;
            var byNationalityList = new List<AdoptedChildrenReportItem>();
            foreach (var nation in nationCountList)
            {
                var enumValue = nationList.FirstOrDefault(i => i.Id == (Guid)nation.Key);
                var name = enumValue != null ? enumValue.Value : "Не указано";
                val = nation.Value;

                var reportItem = new AdoptedChildrenReportItem
                {
                    Name = name,
                    Boys = val[maleEnumItemId],
                    Girls = val[femaleEnumItemId],
                    Total = val[maleEnumItemId] + val[femaleEnumItemId] + val[unknownGenderEnumItemId],
                    No = r
                };
                byNationalityList.Add(reportItem);
                r++;
            }
            var byGeographyList = new List<AdoptedChildrenReportItem>();
            r = 1;
            foreach (var place in placeCountList)
            {
                string name = string.IsNullOrEmpty(place.Key) ? "Не указано" : place.Key;
                val = place.Value;
                var reportItem = new AdoptedChildrenReportItem
                {
                    Name = name,
                    Boys = val[maleEnumItemId],
                    Girls = val[femaleEnumItemId],
                    Total = val[maleEnumItemId] + val[femaleEnumItemId] + val[unknownGenderEnumItemId],
                    No = r
                };
                r++;
                byGeographyList.Add(reportItem);
            }
            report.ByAge = byAges.ToArray();
            report.ByNationalities = byNationalityList.ToArray();
            report.ByGeography = byGeographyList.ToArray();
            return report;
        }

        class ReportItem
        {
            public Guid accountId { get; set; }
            public Guid GenderId { get; set; }
            public Guid NationalityId { get; set; }
            public DateTime? BirthDate { get; set; }
            public string Address { get; set; }
        }
    }
}