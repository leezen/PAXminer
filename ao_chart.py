import pandas as pd
import pymysql.cursors
import configparser
import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import configparser
import sys
import dataframe_image as dfi
import seaborn as sns
import plotly.figure_factory as ff
import traceback

def ao_monthly_summary_chart(mydb, region):
    # Get Current Year, Month Number and Name
    d = datetime.datetime.now()
    d = d - datetime.timedelta(days=3)
    thismonth = d.strftime("%m")
    thismonthname = d.strftime("%b")
    thismonthnamelong = d.strftime("%B")
    yearnum = d.strftime("%Y")

    #Define colormap for table
    cm = sns.light_palette("green", as_cmap=True)

    # Query AWS by for beatdown history
    try:
        mydb.select_db(region)
        with mydb.cursor() as cursor:
            sql = "select av.AO, x.TotalPosts as TotalPosts, count(distinct av.PAX) as TotalUniquePax, count(Distinct av.Date) as BDs, Round(x.TotalPosts/count(Distinct av.Date),1) as AvgAttendance,sum(Distinct bd.fng_count) as TotalFNGs, MONTH(av.Date) as Month,Year(av.date) as Year \
            from attendance_view av \
            left outer join beatdown_info bd on bd.AO = av.ao and bd.date = av.Date \
            left outer join (select sum(pax_count) as TotalPosts, AO, month(Date) as month, year(Date) as Year from beatdown_info GROUP BY year, month, AO) x on x.AO = av.AO and x.month=(month(av.date)) and x.year = (year(av.date)) \
            WHERE Year = %s \
            AND Month = %s \
            GROUP BY year, Month, AO \
            order by Year desc, Month desc, Round(x.TotalPosts/count(Distinct av.Date),1) desc"
            val = (yearnum, thismonth)
            cursor.execute(sql, val)
            bd_tmp = cursor.fetchall()
            bd_tmp_df = pd.DataFrame(bd_tmp)
            bd_tmp_df['Month'] = bd_tmp_df['Month'].replace([1,2,3,4,5,6,7,8,9,10,11,12], ['January','February','March','April','May','June','July','August','September','October','November','December'])
            bd_df_styled = bd_tmp_df.style.background_gradient(cmap=cm, subset=['TotalPosts', 'TotalUniquePax']).set_caption("This region is ON FIRE!")
            filename = './plots/' + region + '/AO_SummaryTable' + thismonthname + yearnum + '.jpg'
            dfi.export(bd_df_styled, filename)  # save the figure to a file
            return filename
    except:
        traceback.print_exc()
    finally:
        pass
