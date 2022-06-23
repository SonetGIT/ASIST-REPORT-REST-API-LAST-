using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Web;

namespace ASIST_REPORT_REST_API.Models
{
    public static class DAL
    {

        const string meta_DB_connectionString = "Data Source=192.168.0.54;Initial Catalog=asist-meta;Password=WestWood-911;Persist Security Info=True;User ID=sa;MultipleActiveResultSets=True;Max Pool Size=3500;Connect Timeout=300;";
        const string data_DB_connectionString = "Data Source=192.168.0.54;Initial Catalog=asist-data;Password=WestWood-911;ersist Security Info=True;User ID=sa;MultipleActiveResultSets=True;Max Pool Size=3500;Connect Timeout=300;";
        const string cissa_DB_connectionString = "Data Source=192.168.0.2;Initial Catalog=cissa;Password=QQQwww123;Persist Security Info=True;User ID=sa;MultipleActiveResultSets=True;Max Pool Size=3500;Connect Timeout=300;";

        public static string GetClassName(Guid id)
        {
            SqlConnection sqlConnection1 = new SqlConnection(meta_DB_connectionString);
            SqlCommand cmd = new SqlCommand();
            //SqlDataReader reader;

            cmd.CommandText = "SELECT Name FROM object_defs WHERE id='" + id.ToString() + "'";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();
            string className = cmd.ExecuteScalar().ToString();
            sqlConnection1.Close();

            return className;
        }

        public static string GetClassName1(Guid id)
        {
            SqlConnection sqlConnection1 = new SqlConnection(meta_DB_connectionString);
            SqlCommand cmd = new SqlCommand();
            //SqlDataReader reader;

            cmd.CommandText = "SELECT Name FROM object_defs WHERE id='" + id.ToString() + "'";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();
            string className = cmd.ExecuteScalar().ToString();
            sqlConnection1.Close();

            return className;
        }

        public class MenuItem
        {
            public Guid Id { get; set; }
            public string NameEN { get; set; }
            public string NameRU { get; set; }
            public int OrderIndex { get; set; }
            public List<MenuItem> Children { get; set; }
        }
        public class Item
        {
            public Guid? parentId { get; set; }
            public Guid elementId { get; set; }
            public string NameEN { get; set; }
            public string NameRU { get; set; }
            public int OrderIndex { get; set; }
        }
        public static List<MenuItem> GetMenuItems()
        {
            var items = new List<Item>();
            SqlConnection sqlConnection1 = new SqlConnection(meta_DB_connectionString);
            SqlCommand cmd = new SqlCommand();
            //SqlDataReader reader;

            cmd.CommandText = @"
select od.Parent_Id 'ParentId', od.Id 'ElementId', od.Name 'NameEN', od.Full_Name 'NameRU', od.Order_Index
from Menus m
inner join Object_Defs od on (od.Id = m.Id)
where od.Full_Name is not null";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    var parentId = reader.IsDBNull(0) ? null : (Guid?)reader.GetGuid(0);
                    var elementId = reader.IsDBNull(1) ? Guid.Empty : reader.GetGuid(1);
                    var nameEng = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
                    var nameRus = reader.IsDBNull(3) ? string.Empty : reader.GetString(3);
                    var orderIndex = reader.IsDBNull(4) ? 0 : reader.GetInt16(4);
                    items.Add(new Item
                    {
                        elementId = elementId,
                        parentId = parentId,
                        NameEN = nameEng,
                        NameRU = nameRus,
                        OrderIndex = orderIndex
                    });
                }
            }



            sqlConnection1.Close();

            return ConvertItemsToMenu(items);
        }
        public class _cissa_user
        {
            public Guid Id { get; set; }
            public string UserName { get; set; }
            public string Password { get; set; }
            public string OrgName { get; set; }
        }
        public static List<_cissa_user> GetCissaUsers()
        {
            var items = new List<_cissa_user>();
            SqlConnection sqlConnection1 = new SqlConnection(meta_DB_connectionString);
            SqlCommand cmd = new SqlCommand();
            //SqlDataReader reader;

            cmd.CommandText = @"
    SELECT users.Id, users.User_Name, users.User_Password, od2.Full_Name
    FROM Workers users
	inner join Object_Defs od on od.Id = users.Id
	inner join Object_Defs od2 on od.Parent_Id = od2.Id
    where users.User_Name not like '%*%'
";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    var userId = reader.IsDBNull(0) ? Guid.Empty : reader.GetGuid(0);
                    var userName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
                    var uPassword = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
                    var uOrgName = reader.IsDBNull(3) ? string.Empty : reader.GetString(3);
                    items.Add(new _cissa_user
                    {
                        Id = userId,
                        UserName = userName,
                        Password = uPassword,
                        OrgName = uOrgName
                    });
                }
            }



            sqlConnection1.Close();

            return items;
        }
        public static List<_cissa_user> GetOldCissaUsers()
        {
            var items = new List<_cissa_user>();
            SqlConnection sqlConnection1 = new SqlConnection(cissa_DB_connectionString);
            SqlCommand cmd = new SqlCommand();
            //SqlDataReader reader;

            cmd.CommandText = @"
    SELECT users.Id, users.User_Name, users.User_Password, od2.Full_Name 'Organization', od3.Full_Name 'Position', od4.Full_Name 'Sector'
    FROM Workers users
	inner join Object_Defs od on od.Id = users.Id
	inner join Object_Defs od2 on od.Parent_Id = od2.Id
	inner join Object_Defs od3 on users.OrgPosition_Id = od3.Id
	inner join Object_Defs od4 on od3.Parent_Id = od4.Id
    where users.User_Name not like '%*%'
";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    var userId = reader.IsDBNull(0) ? Guid.Empty : reader.GetGuid(0);
                    var userName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
                    var uPassword = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
                    var uOrgName = reader.IsDBNull(3) ? string.Empty : reader.GetString(3);
                    items.Add(new _cissa_user
                    {
                        Id = userId,
                        UserName = userName,
                        Password = uPassword,
                        OrgName = uOrgName
                    });
                }
            }



            sqlConnection1.Close();

            return items;
        }

        public static _cissa_user GetCissaUser(Guid userId)
        {
            var items = new List<_cissa_user>();
            SqlConnection sqlConnection = new SqlConnection(meta_DB_connectionString);
            SqlCommand cmd = new SqlCommand();
            //SqlDataReader reader;

            cmd.CommandText = @"
    SELECT users.Id, users.User_Name, users.User_Password
    FROM [cissa-meta].[dbo].[Workers] users
    where users.Id = '" + userId + @"'
";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection;
            var uObj = new _cissa_user();
            sqlConnection.Open();
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.Read())
                {
                    var id = reader.IsDBNull(0) ? Guid.Empty : reader.GetGuid(0);
                    var userName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
                    var uPassword = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
                    uObj.Id = id;
                    uObj.UserName = userName;
                    uObj.Password = uPassword;
                    return uObj;
                }
            }

            sqlConnection.Close();
            return null;
        }
        public static _cissa_user GetOldCissaUser(Guid userId)
        {
            var items = new List<_cissa_user>();
            SqlConnection sqlConnection = new SqlConnection(cissa_DB_connectionString);
            SqlCommand cmd = new SqlCommand();
            //SqlDataReader reader;

            cmd.CommandText = @"
    SELECT users.Id, users.User_Name, users.User_Password
    FROM [cissa].[dbo].[Workers] users
    where users.Id = '" + userId + @"'
";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection;
            var uObj = new _cissa_user();
            sqlConnection.Open();
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.Read())
                {
                    var id = reader.IsDBNull(0) ? Guid.Empty : reader.GetGuid(0);
                    var userName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
                    var uPassword = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
                    uObj.Id = id;
                    uObj.UserName = userName;
                    uObj.Password = uPassword;
                    return uObj;
                }
            }

            sqlConnection.Close();
            return null;
        }

        private static List<MenuItem> ConvertItemsToMenu(List<Item> items)
        {
            var menuItems = new List<MenuItem>();

            foreach (var item in items.Where(x => x.parentId == null))
            {
                var mItem = new MenuItem
                {
                    Id = item.elementId,
                    NameEN = item.NameEN,
                    NameRU = item.NameRU,
                    OrderIndex = item.OrderIndex,
                    Children = new List<MenuItem>()
                };
                InitChildren(items, mItem);
                menuItems.Add(mItem);
            }

            return menuItems;
        }
        private static void InitChildren(List<Item> items, MenuItem element)
        {
            foreach (var subItem in items.Where(x => x.parentId == element.Id))
            {
                var subMenuItem = new MenuItem
                {
                    Id = subItem.elementId,
                    NameEN = subItem.NameEN,
                    NameRU = subItem.NameRU,
                    OrderIndex = subItem.OrderIndex,
                    Children = new List<MenuItem>()
                };
                InitChildren(items, subMenuItem);
                element.Children.Add(subMenuItem);
            }
        }
    }
}