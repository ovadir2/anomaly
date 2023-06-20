import pandas as pd
# import urllib
# import urllib.parse
# import sqlalchemy as sa
import pyarrow as pa
#import pyarrow.hdfs as hdfs
import pyarrow.filesystem as fs

# from sqlalchemy.sql import text
# from sqlalchemy.exc import OperationalError 
# from sqlalchemy.exc import SQLAlchemyError
import pyodbc
# from pyspark.sql.functions import current_timestamp
# #from sqlalchemy import create_engine
# #from  append_hdfs_parquet import append_hdfs_parquet
import io
  
#configs_id for sealingcell is '917'
def fetch_sealing_data(year=2023, quarter=None, month=None, yearweek=None, weekday=None, configs_id=917):
    
   
    _filter =''
    if quarter:
        _filter = f' AND quarter = {quarter}'
        
    else:
        if month: #overwrite quarter
            _filter = f' AND month = {month}'
        else:
            if yearweek:  #overwrite month
                _filter = f' AND week = {yearweek}'
            if weekday:  #add day in week
                    _filter += f' AND dayweek = {weekday}'
         
    #print(f'filter is: {_filter}')
    try:
       with  pyodbc.connect("DRIVER={ODBC Driver 18 for SQL Server};\
                       server=10.247.232.23,1433;UID=ovadir2;\
                       PWD=1qaz!2wsx@;;TrustServerCertificate=yes;\
                       database=CQCAnalyzer;") as con:
   
            fquery1 = (f"""WITH CTE AS ( SELECT B.BatchID, B.BatchName, B.BatchPath, B.ConfigID, B.Shift, B.Cell_Name, B.TestTypeID,
                                   B.LastInsertedRow, B.StartTime, B.Heads, B.BatteryTypeID, B.BodyTypeID, B.DomeTypeID,
                                   B.PCATypeID, B.PCBTypeID, B.PlacementContractorID, B.EmployeID, B.OpticsTypeID,
                                   B.Versions, B.LotNumber, B.SubLot, TP.TestID, TP.Capsule_Number,
                                   TP.Initial_Capsule_Id_HEX, TP.Capsule_Id_HEX, TP.Test_Counter, TP.ERROR_Code_Number,
                                   TP.Capsule_Id, TP.PASS_FAILED, TP.Start_Time, TP.End_time, TP.Test_Time_min, TP.Date,
                                   TP.History, TP.Remark, TP.Blister_Id, TP.Fail_Tray, TP.Fail_capsule_position,
                                   TP.ERROR_Code_Description, TP.WTC, TP.CQC_num, TP.Capsule_Counter, TP.Cell_Name AS TP_Cell_Name,
                                   SC.DomeCaseGap, SC.LaserPower, SC.StitchArea, SC.MinStitchWidth, SC.LeakTest,
                                   DATEPART(YEAR, CONVERT(DATE, TP.Date, 103)) AS year,
                                   DATEPART(QQ, CONVERT(DATE, TP.Date, 103)) AS quarter,
                                   DATEPART(WEEK, CONVERT(DATE, TP.Date, 103)) AS week,
                                   DATEPART(MONTH, CONVERT(DATE, TP.Date, 103)) AS month,
                                   DATEPART(DW, CONVERT(DATE, TP.Date, 103)) AS dayweek 
                                    FROM dbo.Batch AS B
                                    JOIN dbo.TestProperty AS TP ON B.BatchID = TP.BatchID
                                    JOIN SealingCell AS SC ON 
                       
                       
                       TP.TestID = SC.TestID
                                    WHERE B.ConfigID = {configs_id} )
            
                                    SELECT CTE.*,
                                           CTE.DomeCaseGap AS DomeCaseGap_spc_orig,
                                           CTE.LaserPower AS LaserPower_spc_orig,
                                           CTE.MinStitchWidth AS MinStitchWidth_spc_orig,
                                           CTE.StitchArea AS StitchArea_spc_orig,
                                           CTE.LeakTest AS LeakTest_spc_orig,
                                           CONCAT((CTE.DomeCaseGap * 0.8), ':', (CTE.DomeCaseGap * 1.2)) AS DomeCaseGap_spc,
                                           CONCAT((CTE.LaserPower * 0.8), ':', (CTE.LaserPower * 1.2)) AS LaserPower_spc,
                                           CONCAT((CTE.MinStitchWidth * 0.8), ':', (CTE.MinStitchWidth * 1.2)) AS MinStitchWidth_spc,
                                           CONCAT((CTE.StitchArea * 0.8), ':', (CTE.StitchArea * 1.2)) AS StitchArea_spc,
                                           CONCAT((CTE.LeakTest * 0.8), ':', (CTE.LeakTest * 1.2)) AS LeakTest_spc,
                                           limits.*
                                    FROM CTE
                                    CROSS APPLY ( SELECT scl.BatchId,
                                                 scl.DomeCaseGap AS DomeCaseGap_limit,
                                                 scl.LaserPower AS LaserPower_limit,
                                                 scl.MinStitchWidth AS MinStitchWidth_limit,
                                                 scl.StitchArea AS StitchArea_limit,
                                                 scl.LeakTest AS LeakTest_limit
                                        FROM dbo.SealingCellLimits AS scl
                                        WHERE scl.BatchId = CTE.BatchID) AS limits
                                    WHERE year = {year} {_filter} ;""")
            
            df = pd.read_sql(fquery1, con)
             

            # print('=======================================')
            # print(' LTD_BRAVO stations data......completed')
            # print('=======================================')
            return df

    except (pyodbc.OperationalError) as error:
        print(f"Error connecting to SQL Server Bravo: {error}")
        return pd.DataFrame()

if __name__ == '__main__':

    json_path = "/home/naya/anomaly/files_json/scd_raw.json"


    scd_raw = fetch_sealing_data(year=2022, quarter=None, month=None, yearweek=None, weekday=None, configs_id=917)
    # Rename the duplicate column
    scd_raw.columns = ['_BatchID' if col == 'BatchID' else col for col in scd_raw.columns]
    print(scd_raw)
    scd_raw.to_json(json_path)