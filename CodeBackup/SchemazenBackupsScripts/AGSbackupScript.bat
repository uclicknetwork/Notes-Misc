SchemaZen.exe script --server UCLICKSERVER05\UCLICKSERVER05 --database UC_Admin --scriptDir G:\Uclick_Product_Suite\CodeBackup\ReferenceServer\UC_Admin --overwrite
SchemaZen.exe script --server UCLICKSERVER05\UCLICKSERVER05 --database UC_Commerce --scriptDir G:\Uclick_Product_Suite\CodeBackup\ReferenceServer\UC_Commerce --overwrite
SchemaZen.exe script --server UCLICKSERVER05\UCLICKSERVER05 --database UC_Operations --scriptDir G:\Uclick_Product_Suite\CodeBackup\ReferenceServer\UC_Operations --overwrite
SchemaZen.exe script --server UCLICKSERVER05\UCLICKSERVER05 --database UC_Reference --scriptDir G:\Uclick_Product_Suite\CodeBackup\ReferenceServer\UC_Reference --overwrite

SchemaZen.exe script --server UCLICKSERVER06\UCLICKSERVER06 --user=uclickadmin --pass=CCPL@2017 --database UC_Report --scriptDir G:\Uclick_Product_Suite\CodeBackup\ReportServer\UC_Report --overwrite

SchemaZen.exe script --server UCLICKSERVER04\UCLICKSERVER04 --user=uclickadmin --pass=CCPL@2017 --database UC_Facilitate --scriptDir G:\Uclick_Product_Suite\CodeBackup\BridgeServer\UC_Facilitate --overwrite
SchemaZen.exe script --server UCLICKSERVER04\UCLICKSERVER04 --user=uclickadmin --pass=CCPL@2017 --database UC_Bridge --scriptDir G:\Uclick_Product_Suite\CodeBackup\BridgeServer\UC_Bridge --overwrite

SchemaZen.exe script --server UCLICKSERVER01\UCLICKSERVER01 --user=uclickadmin --pass=CCPL@2017 --database UC_Data01 --scriptDir G:\Uclick_Product_Suite\CodeBackup\CDRServer\UC_Data01 --overwrite



"c:\Program Files\7-Zip\7z.exe" a AGS.zip ReferenceServer ReportServer BridgeServer CDRServer
RD /S /Q ReferenceServer ReportServer BridgeServer CDRServer
