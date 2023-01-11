import configparser
import pyodbc
import sys
import pandas as pd
def connectWithDB():
    config = configparser.ConfigParser()
    config.read('sqlconfig.config')
    host = config['Database1']['host']
    database = config['Database1']['database']
    cnxn_str = ("Driver={SQL Server Native Client 11.0};"
                f"""Server={host};"""
                f"""Database={database};"""
                "Trusted_Connection=yes;")
    try:
        cnxn = pyodbc.connect(cnxn_str)
        print("Successfull")
        return cnxn
    except Exception as e:
        print(e)
        print('DB connection FAILED, please check your connection string')
        sys.exit()

def GetCropsData():
    try:
        pd.set_option('display.max_columns', None)
        df = pd.read_csv(r'D:\Tellymarks\Flood Assesment\Survey_data_Crops.txt', sep='\t')
        df.to_csv(r'D:\Tellymarks\Flood Assesment\Survey_data_Crops.csv',index=False,header=True)
        connectDB = connectWithDB()
        cursor = connectDB.cursor()
        for index,row in df.iterrows():
            print(type(row.trees_all_types))
            print(row.losses_of_live_stock)
            cursor.execute("INSERT INTO [dbo].[survey_data_crops] (ID,PrimaryPath,Picture1,Picture2,DateTime,UserID,DestinationType,District,Tehsil,Name,FatherSpouse,CNIC,Age,Address,Mobile,NameOfKin,CNICDeceased,Remarks,KindOfDamage,TreesAllTypes,LossesOfLiveStock,DateTimeMobile,Lat,Long,Geom,Gender,MaritalStatus,RelationWithDeceased,WatarCanalMeter,OccupancyStatus,LocationRisk,FloodPalin,BankAccount,Division) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CAST(? AS VARCHAR),CAST(? AS VARCHAR),CAST(? AS VARCHAR),?,?,?,?,?,?,?,?,?,?,?,?)", (str(row.id), str(row.primary_path), str(row.picture1),str(row.picture2),str(row.db_date_time),str(row.user_id),str(row.destruction_type),str(row.district),str(row.tehsil),str(row.name),str(row.father_spouse),str(row.cnic),str(row.age),str(row.address),str(row.mobile),str(row.name_of_kin),str(row.cnic_deceased),str(row.remarks),str(row.kind_of_damage),str(row.trees_all_types),str(row.losses_of_live_stock),str(row.date_time_mobile),str(row.lat),str(row.lng),str(row.geom),str(row.gender),str(row.marital_status),str(row.relation_with_deceased),str(row.watar_canal_meter),str(row.occupancy_status),str(row.location_risk),str(row.flood_palin),str(row.bank_account),str(row.division)))
    except Exception as e:
        print(e)
    finally:
        cursor.commit()
        cursor.close()
        connectDB.close()
def GetPropertiesData():
    try:
        pd.set_option('display.max_columns', None)
        df = pd.read_csv(r'C:\Users\Huzaifa\Downloads\Survey_data_Properties.txt', sep='\t')
        df.to_csv(r'C:\Users\Huzaifa\Downloads\Survey_data_Properties.csv',index=False,header=True)
        connectDB = connectWithDB()
        cursor = connectDB.cursor()
        for index,row in df.iterrows():
            print(type(row.trees_all_types))
            print(row.losses_of_live_stock)
            cursor.execute("INSERT INTO [dbo].[survey_data_crops] (ID,PrimaryPath,Picture1,Picture2,DateTime,UserID,DestinationType,District,Tehsil,Name,FatherSpouse,CNIC,Age,Address,Mobile,NameOfKin,CNICDeceased,Remarks,KindOfDamage,TreesAllTypes,LossesOfLiveStock,DateTimeMobile,Lat,Long,Geom,Gender,MaritalStatus,RelationWithDeceased,WatarCanalMeter,OccupancyStatus,LocationRisk,FloodPalin,BankAccount,Division) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CAST(? AS VARCHAR),CAST(? AS VARCHAR),CAST(? AS VARCHAR),?,?,?,?,?,?,?,?,?,?,?,?)", (str(row.id), str(row.primary_path), str(row.picture1),str(row.picture2),str(row.db_date_time),str(row.user_id),str(row.destruction_type),str(row.district),str(row.tehsil),str(row.name),str(row.father_spouse),str(row.cnic),str(row.age),str(row.address),str(row.mobile),str(row.name_of_kin),str(row.cnic_deceased),str(row.remarks),str(row.kind_of_damage),str(row.trees_all_types),str(row.losses_of_live_stock),str(row.date_time_mobile),str(row.lat),str(row.lng),str(row.geom),str(row.gender),str(row.marital_status),str(row.relation_with_deceased),str(row.watar_canal_meter),str(row.occupancy_status),str(row.location_risk),str(row.flood_palin),str(row.bank_account),str(row.division)))
    except Exception as e:
        print(e)
    finally:
        cursor.commit()
        cursor.close()
        connectDB.close()

def main():
    GetCropsData()
    GetPropertiesData()

if __name__ == "__main__":
    main()
