using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;

namespace GMTReportReadiness
{
    class Program
    {
        DateTime GMTpredefined;
        DateTime GMTolap;
        DateTime GMTdatafeed;

        DateTime GMTpredefinedSLA = DateTime.Parse(DateTime.Today.ToString("MM/dd/yyyy") + " " + "08:00:00");
        DateTime GMTolapSLA = DateTime.Parse(DateTime.Today.ToString("MM/dd/yyyy") + " " + "09:00:00");
        DateTime GMTdatafeedSLA = DateTime.Parse(DateTime.Today.ToString("MM/dd/yyyy") + " " + "08:00:00");
        DateTime GMTdatafeedSLACutOff = DateTime.Parse(DateTime.Today.ToString("MM/dd/yyyy") + " " + "07:55:00");
        DateTime Xaxis730SLA = DateTime.Parse(DateTime.Today.ToString("MM/dd/yyyy") + " " + "07:30:00");

        static void Main(string[] args)
        {
            Program p = new Program();
            p.checkBDB();
            p.checkDatafeed();
            Console.WriteLine("Email notifications sent to nocsupport@sizmek.com");
            
        }
        private SqlConnection connectToDB(string id, string password, string server, string database)
        {
            return new SqlConnection("user id=" + id + ";" + "password=" + password + ";" + "server=" + server + ";" + "Trusted_Connection=false;" + "database=" + database + ";" + "connection timeout=30");
        }
        private void checkBDB()
        {
            SqlDataReader myReader = null;
            SqlConnection conn = new SqlConnection("user id=" + "readonlyuser" + ";" + "password=" + "rou123" + ";" + "server=" + "BDBNJ.eyedcny.local" + ";" + "Trusted_Connection=false;" + "database=" + "master" + ";" + "connection timeout=30");
            conn.Open();
            //string stmt5 = "SELECT TimeZoneID , Description , BurstingDB.dbo.DateIDToDateTime(LastAggDateID) AS PTM_API_AggDatafeed_Currently_Availabe_Data , DATEADD(dd,-1, BurstingDB.dbo.DateIDToDateTime(LastCRBSyncRepDateUpdateDateIDIncludeHour / 100 * 100)) AS CRB_VA_Currently_Availabe_Data FROM BurstingDB..TimeZoneLookup WHERE AdditionalTimeZoneSupported = 1 AND Description = 'GMT Greenwich Mean Time';";
            string stmt5 = "SELECT [TimeZoneID] ,[GMT] ,[LastAggDateID] ,[Description] ,[EST] ,[LastVAAggDateID] ,[LastVAPublishDate] ,[VaAggUpdateTime] ,[AggUpdateTime] FROM [BurstingDB].[dbo].[TimeZoneLookup] WHERE AdditionalTimeZoneSupported = 1 AND Description = 'GMT Greenwich Mean Time';";
            SqlCommand comm4 = new SqlCommand(stmt5, conn);
            myReader = comm4.ExecuteReader();
            while (myReader.Read())
            {
                /*
                DateTime predefinedDate = DateTime.Parse(myReader["PTM_API_AggDatafeed_Currently_Availabe_Data"].ToString().Split(' ')[0]);
                predefinedReports.InnerText = predefinedDate.ToString("dd/MM/yyyy");
                DateTime olapDate = DateTime.Parse(myReader["CRB_VA_Currently_Availabe_Data"].ToString().Split(' ')[0]);
                olapReports.InnerText = olapDate.ToString("dd/MM/yyyy");
                */

                GMTpredefined = DateTime.Parse(myReader["AggUpdateTime"].ToString());
                GMTolap = DateTime.Parse(myReader["VaAggUpdateTime"].ToString());

                DateTime predefinedDate = DateTime.Parse(myReader["AggUpdateTime"].ToString().Split(' ')[0]);
                DateTime olapDate = DateTime.Parse(myReader["VaAggUpdateTime"].ToString().Split(' ')[0]);

                //time in gmt
                DateTime utc = DateTime.UtcNow;

                double GMTPredefinedSLAdiff = utc.Subtract(GMTpredefinedSLA).TotalMinutes;
                TimeSpan GMTpredefinedDiff = GMTpredefined.Subtract(GMTpredefinedSLA);


                //if the predefined reports are still not ready
                if (predefinedDate != DateTime.Today)
                {
                    sendMail("GMT Predefined is not yet ready", "GMT Predefined report is not yet ready.");
                }
                /*
                else // the predefined reports are ready
                {

                    if (GMTpredefinedDiff.TotalMinutes > 0) //Check if we surpassed the internal SLA
                    {
                        sendMail("GMT Predefined is now ready", "GMT Predefined report were delivered with delay.");
                    }
                    else //they were ready before the internal SLA
                    {
                        sendMail("GMT Predefined is now ready", "GMT Predefined report is now ready.");
                    }
                }
                */

                double GMTOlapSLAdiff = utc.Subtract(GMTolapSLA).TotalMinutes;
                TimeSpan GMTolapDiff = GMTolap.Subtract(GMTolapSLA);

                //if the OLAP reports are still not ready
                if (olapDate != DateTime.Today)
                {
                    sendMail("GMT OLAP is not yet ready", "GMT OLAP report is not yet ready.");
                }
                /*
                else
                {
                    if (GMTolapDiff.TotalMinutes > 0)
                    {
                        sendMail("GMT OLAP is now ready", "GMT OLAP report were delivered with delay.");
                    }
                    else
                    {
                        sendMail("GMT OLAP is now ready", "GMT OLAP report is now ready.");
                    }
                }
                */

            }
            myReader.Close();
            conn.Close();
        }

        private void checkDatafeed()
        {
            SqlDataReader myReader = null;
            SqlConnection conn = new SqlConnection("user id=" + "readonlyuser" + ";" + "password=" + "rou123" + ";" + "server=" + "RSDWHNJ.eyedcny.local" + ";" + "Trusted_Connection=false;" + "database=" + "Reportsdwh" + ";" + "connection timeout=30");
            conn.Open();
            string stmt5 = "SELECT FeedName, MAX(AvailabeSince) AS AvailabeSince ,MAX(LastDateAvailabe) AS LastDateAvailabe FROM dbo.DataFeeds_Status_ByAccount_Detailed WHERE (FeedName LIKE '%Xaxis DE%' or FeedName  like '%AllAccount%GMT%') AND LastDateAvailabe < GetDate() GROUP BY FeedName;";
            SqlCommand comm4 = new SqlCommand(stmt5, conn);
            myReader = comm4.ExecuteReader();
            while (myReader.Read())
            {

                if (myReader["FeedName"].ToString() == "GroupM_DE_AllAccount_GP_GMT")
                {
                    GMTdatafeed = DateTime.Parse(myReader["AvailabeSince"].ToString());

                    DateTime datafeedDate = DateTime.Parse(myReader["AvailabeSince"].ToString().Split(' ')[0]);

                    //find out what time it is in EST
                    var timeUtc = DateTime.UtcNow;
                    TimeZoneInfo easternZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
                    DateTime easternTime = TimeZoneInfo.ConvertTimeFromUtc(timeUtc, easternZone);

                    //time in gmt
                    DateTime utc = DateTime.UtcNow;

                    //find the difference between them
                    TimeSpan GMTandESTdiff = utc.Subtract(easternTime);

                    DateTime datafeedTimeInGMT = DateTime.Parse(myReader["AvailabeSince"].ToString()).AddHours(Math.Abs(GMTandESTdiff.TotalHours));

                    double GMTSLAdiff = utc.Subtract(GMTdatafeedSLA).TotalMinutes;

                    //TimeSpan GMTdatafeedDiff = GMTdatafeed.Subtract(GMTdatafeedSLA);
                    TimeSpan GMTdatafeedDiff = datafeedTimeInGMT.Subtract(GMTdatafeedSLA);
                    if (DateTime.Parse(datafeedTimeInGMT.ToString().Split(' ')[0]) != DateTime.Today)
                    {
                        Console.WriteLine("GroupM_DE_AllAccount_GP_GMT Not Ready");
                        sendMail("GroupM_DE_AllAccount_GP_GMT Not Ready", "GroupM DE AllAccount GP GMT is not yet ready.");
                    }
                    /*
                    else
                    {
                        if (GMTdatafeedDiff.TotalMinutes > 0)
                        {
                            sendMail("GroupM_DE_AllAccount_GP_GMT is now Ready", "GroupM DE AllAccount GP GMT is now ready and were delivered with delay.");

                        }
                        else
                        {
                            sendMail("GroupM_DE_AllAccount_GP_GMT is now Ready", "GroupM DE AllAccount GP GMT is now ready.");
                        }
                    }
                    */
                }
                else if (myReader["FeedName"].ToString() == "GroupM_DE_AllAccount_WinningEvent_GMT")
                {
                    GMTdatafeed = DateTime.Parse(myReader["AvailabeSince"].ToString());

                    DateTime datafeedDate = DateTime.Parse(myReader["AvailabeSince"].ToString().Split(' ')[0]);

                    //find out what time it is in EST
                    var timeUtc = DateTime.UtcNow;
                    TimeZoneInfo easternZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
                    DateTime easternTime = TimeZoneInfo.ConvertTimeFromUtc(timeUtc, easternZone);

                    //time in gmt
                    DateTime utc = DateTime.UtcNow;

                    //find the difference between them
                    TimeSpan GMTandESTdiff = utc.Subtract(easternTime);

                    DateTime datafeedTimeInGMT = DateTime.Parse(myReader["AvailabeSince"].ToString()).AddHours(Math.Abs(GMTandESTdiff.TotalHours));

                    double GMTSLAdiff2 = utc.Subtract(GMTdatafeedSLA).TotalMinutes;

                    //TimeSpan GMTdatafeedDiff = GMTdatafeed.Subtract(GMTdatafeedSLA);
                    TimeSpan GMTdatafeedDiff2 = datafeedTimeInGMT.Subtract(GMTdatafeedSLA);

                    if (DateTime.Parse(datafeedTimeInGMT.ToString().Split(' ')[0]) != DateTime.Today)
                    {
                        sendMail("GroupM_DE_AllAccount_WinningEvent_GMT is not yet ready", "GroupM_DE_AllAccount_WinningEvent_GMT is not yet ready.");
                    }
                    /*
                    else
                    {

                        if (GMTdatafeedDiff2.TotalMinutes > 0)
                        {
                            sendMail("GroupM_DE_AllAccount_WinningEvent_GMT is now ready", "GroupM_DE_AllAccount_WinningEvent_GMT is now ready and were delivered with delay.");
                        }
                        else
                        {
                            sendMail("GroupM_DE_AllAccount_WinningEvent_GMT is now ready", "GroupM_DE_AllAccount_WinningEvent_GMT is now ready.");
                        }

                    }
                    */
                }
                else if (myReader["FeedName"].ToString() == "1_Xaxis DE - Daily feed")
                {
                    GMTdatafeed = DateTime.Parse(myReader["AvailabeSince"].ToString());

                    DateTime datafeedDate = DateTime.Parse(myReader["AvailabeSince"].ToString().Split(' ')[0]);

                    //find out what time it is in EST
                    var timeUtc = DateTime.UtcNow;
                    TimeZoneInfo easternZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
                    DateTime easternTime = TimeZoneInfo.ConvertTimeFromUtc(timeUtc, easternZone);

                    //time in gmt
                    DateTime utc = DateTime.UtcNow;

                    //find the difference between them
                    TimeSpan GMTandESTdiff = utc.Subtract(easternTime);

                    DateTime datafeedTimeInGMT = DateTime.Parse(myReader["AvailabeSince"].ToString()).AddHours(Math.Abs(GMTandESTdiff.TotalHours));


                    double GMTSLAdiff = utc.Subtract(GMTdatafeedSLA).TotalMinutes;

                    TimeSpan GMTdatafeedDiff = datafeedTimeInGMT.Subtract(GMTdatafeedSLA);
                    if (DateTime.Parse(datafeedTimeInGMT.ToString().Split(' ')[0]) != DateTime.Today)
                    {
                        sendMail("Xaxis DE - Daily feed is not yet ready", "Xaxis DE - Daily feed is not yet ready.");
                    }
                    /*
                    else
                    {

                        if (GMTdatafeedDiff.TotalMinutes > 0)
                        {
                            sendMail("Xaxis DE - Daily feed is now ready", "Xaxis DE - Daily feed is now ready and were delivered with delay.");

                        }
                        else
                        {
                            sendMail("Xaxis DE - Daily feed is now ready", "Xaxis DE - Daily feed is now ready.");
                        }
                    }
                    */
                }
            }
            myReader.Close();
            conn.Close();
        }
        private void sendMail(string emailSubject, string emailBody)
        {
            MailMessage mailMessage = new MailMessage("xaxisreports@sizmek.com", "nocsupport@sizmek.com");
            mailMessage.Subject = emailSubject;
            mailMessage.Body = emailBody;

            SmtpClient smtpClient = new SmtpClient("10.10.2.15", 25);
            smtpClient.EnableSsl = false;
            smtpClient.Send(mailMessage);
        }
    }
}
