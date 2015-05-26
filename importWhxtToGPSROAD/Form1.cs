using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace importWhxtToGPSROAD
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            OpenFileDialog fd = new OpenFileDialog();
            fd.Title = "选择数据";
            fd.Filter = "(*.mdb)|*.mdb";
            fd.Multiselect = false;
            if (fd.ShowDialog() != DialogResult.OK) return;
            textBox1.Text = fd.FileName;
           
        }

        private void mergeTable(DataTable source, DataTable add)
        {
            foreach (DataRow adddr in add.Rows)
            {

                DataRow dr = source.NewRow();
                foreach (DataColumn dc in add.Columns)
                {
                    if (source.Columns.Contains(dc.ColumnName))
                    {
                        dr[dc.ColumnName] = adddr[dc.ColumnName];
                    }
                }
                dr["ID"] = Guid.NewGuid();
                source.Rows.Add(dr);
            }
        }

        private void mergeTableNonewid(DataTable source, DataTable add)
        {
            foreach (DataRow adddr in add.Rows)
            {

                DataRow dr = source.NewRow();
                foreach (DataColumn dc in add.Columns)
                {
                    if (source.Columns.Contains(dc.ColumnName))
                    {
                        dr[dc.ColumnName] = adddr[dc.ColumnName];
                    }
                }
                //dr["ID"] = Guid.NewGuid();
                source.Rows.Add(dr);
            }
        }
        private void button2_Click(object sender, EventArgs e)
        {
            string path = textBox1.Text;
            string conAccessstr = "Provider=Microsoft.Jet.OleDb.4.0;Data Source=" + path + ";Jet OLEDB:DataBase Password=Hdsxtech好8668";
            string dbstr = @"Data Source=KEPTON\SQL2008;Initial Catalog=NCGLBCDC_440000_2014;User ID=sa;Password=sasa;";
            List<string> gpsroads = new List<string>();
            gpsroads.Add("GPSGD");
            gpsroads.Add("GPSSD");
            gpsroads.Add("GPSXD");
            gpsroads.Add("GPSYD");
            gpsroads.Add("GPSZD");
            gpsroads.Add("GPSCD");
            gpsroads.Add("GPSND");
            gpsroads.Add("GPSVD");
            gpsroads.Add("GPSGS");
            SqlConnection connection = new SqlConnection(dbstr);
            connection.Open();
            using (OleDbConnection OleConn = new OleDbConnection(conAccessstr))
            {
                OleConn.Open();
                foreach (string item in gpsroads)
                {
                    string xzdj = "";
                    switch (item)
                    {
                        case "GPSGD":
                        case "GPSGS":
                            xzdj = "高速";
                            break;
                        case "GPSSD":
                            xzdj = "省道";
                            break;
                        case "GPSXD":
                            xzdj = "县道";
                            break;
                        case "GPSYD":
                            xzdj = "乡道";
                            break;
                        case "GPSZD":
                            xzdj = "专道";
                            break;
                        case "GPSCD":
                            xzdj = "村道";
                            break;
                        case "GPSND":
                            xzdj = "拟建道路";
                            break;
                        case "GPSVD":
                            xzdj = "新增道路";
                            break;
                    }
                    /*string sql =@"SELECT ID,left(roadcode,len(roadcode)-6)as roadcode1,right(roadcode,6)as xzqh,roadname,distcode as xzbm,distname as xzmc,
                                         startname as qddm,startzh,endzh,endname as zddm,roadstart,roadstart_,roadends,roadends_,startfj as qdfjdlb,endfj as zdfjdlb,ldlx
                                         ,ldxz,lmlx,dimao as dm,sfxz,'"+xzdj+@"'as xzdj,shape
                                         from  "+item;*/
                    string sql = @"SELECT ID,left(roadcode,len(roadcode)-6)as roadcode,right(roadcode,6)as xzqh,roadname,distcode as xzbm,distname as xzmc,
                                        startname as qddm,startzh,endzh,endname as zddm,roadstart,lmkd,ljkd,
                                        roadstart_,roadends,roadends_,startfj as qdfjdlb,endfj as zdfjdlb,ldlx
                                        ,ldxz,lmlx,dimao as dm,sfxz,shape,
                                        '" + xzdj+@"'as xzdj
                                        from  "+item;


                    if (item == "GPSND")
                    {
                        sql = @"SELECT ID,left(roadcode,len(roadcode)-6)as roadcode,right(roadcode,6)as xzqh,roadname,distcode as xzbm,distname as xzmc,
                                        startname as qddm,startzh,endzh,endname as zddm,roadstart,lmkd,ljkd,
                                        roadstart_,roadends,roadends_,startfj as qdfjdlb,endfj as zdfjdlb,ldlx
                                        ,ldxz,lmlx,dimao as dm,shape,
                                        '" + xzdj + @"'as xzdj
                                        from  " + item;
                    }

                    OleDbDataAdapter ExcelDA = new OleDbDataAdapter(sql, OleConn);
                    DataSet ExcelDS = new DataSet();
                    ExcelDA.Fill(ExcelDS);

                 
                    try
                    {
                      
                        SqlCommand targetCommand = connection.CreateCommand();
                        SqlDataAdapter sda = new SqlDataAdapter(targetCommand);
                        SqlCommandBuilder builder = new SqlCommandBuilder(sda);
                        targetCommand.CommandText = string.Format("select * from GPSROAD where 1<>1");
                        
                        DataTable targetTable = new DataTable();
                        sda.Fill(targetTable);

                        mergeTable(targetTable, ExcelDS.Tables[0]);

                        sda.Update(targetTable);
                    }
                    catch (Exception ex)
                    {
                        throw (ex);
                    }
                   
                }
                MessageBox.Show("导入完成");
            }
          
        }

        private void button3_Click(object sender, EventArgs e)
        {
            string path = textBox1.Text;
            string conAccessstr = "Provider=Microsoft.Jet.OleDb.4.0;Data Source=" + path + ";Jet OLEDB:DataBase Password=Hdsxtech好8668";
            string dbstr = @"Data Source=KEPTON\SQL2008;Initial Catalog=NCGLBCDC_440000_2014;User ID=sa;Password=sasa;";
            SqlConnection connection = new SqlConnection(dbstr);
            if(connection.State!=ConnectionState.Open)
                    connection.Open();
            OleDbConnection OleConn = new OleDbConnection(conAccessstr);
            if (OleConn.State != ConnectionState.Open)
                OleConn.Open();
            string jianzhicunsql = @"select id,left(vallagebm,6)as xzqh,right(vallagebm,6)as code,name,renkou as jzcrk,dixing as ssdx,islaoqu as sfgmlq
            ,isshaoshu as sfssmzjjq,ispinkun as sfpkdq,isshangbao as sfsb,tdxzh as tdxz,ptx,pty
            from jianzhicun";

            OleDbDataAdapter ExcelDA = new OleDbDataAdapter(jianzhicunsql, OleConn);
            DataSet ExcelDS = new DataSet();
            ExcelDA.Fill(ExcelDS);

            SqlCommand targetCommand = connection.CreateCommand();
            SqlDataAdapter sda = new SqlDataAdapter(targetCommand);
            SqlCommandBuilder builder = new SqlCommandBuilder(sda);
            targetCommand.CommandText = string.Format("select * from jianzhicun where 1<>1");

            DataTable targetTable = new DataTable();
            sda.Fill(targetTable);

            mergeTableNonewid(targetTable, ExcelDS.Tables[0]);

            sda.Update(targetTable);
            MessageBox.Show("导入完成");
        }

        private void button4_Click(object sender, EventArgs e)
        {
            string path = textBox1.Text;
            string conAccessstr = "Provider=Microsoft.Jet.OleDb.4.0;Data Source=" + path + ";Jet OLEDB:DataBase Password=Hdsxtech好8668";
            string dbstr = @"Data Source=KEPTON\SQL2008;Initial Catalog=NCGLBCDC_440000_2014;User ID=sa;Password=sasa;";
            SqlConnection connection = new SqlConnection(dbstr);
            if (connection.State != ConnectionState.Open)
                connection.Open();
            OleDbConnection OleConn = new OleDbConnection(conAccessstr);
            if (OleConn.State != ConnectionState.Open)
                OleConn.Open();
            string jianzhicunsql = @"select id,distid,roadcode,roadname,right(roadcode,6)as xzqh,tdshd as tdwz,isoklu as sfsyxlx,tdfx,jianjienb as Sfljxznbjd
            from jianzhicuntd";

            OleDbDataAdapter ExcelDA = new OleDbDataAdapter(jianzhicunsql, OleConn);
            DataSet ExcelDS = new DataSet();
            ExcelDA.Fill(ExcelDS);

            SqlCommand targetCommand = connection.CreateCommand();
            SqlDataAdapter sda = new SqlDataAdapter(targetCommand);
            SqlCommandBuilder builder = new SqlCommandBuilder(sda);
            targetCommand.CommandText = string.Format("select * from jianzhicuntd where 1<>1");

            DataTable targetTable = new DataTable();
            sda.Fill(targetTable);

            mergeTableNonewid(targetTable, ExcelDS.Tables[0]);

            sda.Update(targetTable);
            MessageBox.Show("导入完成");
        }
    }
}
