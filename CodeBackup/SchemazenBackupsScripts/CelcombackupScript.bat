SchemaZen.exe script --server UCLICKSERVER05\REFERENCESERVER --database UC_Admin --scriptDir G:\Uclick_Product_Suite\CodeBackup\ReferenceServer\UC_Admin --overwrite
SchemaZen.exe script --server UCLICKSERVER05\REFERENCESERVER --database UC_Commerce --scriptDir G:\Uclick_Product_Suite\CodeBackup\ReferenceServer\UC_Commerce --overwrite
SchemaZen.exe script --server UCLICKSERVER05\REFERENCESERVER --database UC_Operations --scriptDir G:\Uclick_Product_Suite\CodeBackup\ReferenceServer\UC_Operations --overwrite
SchemaZen.exe script --server UCLICKSERVER05\REFERENCESERVER --database UC_Reference --scriptDir G:\Uclick_Product_Suite\CodeBackup\ReferenceServer\UC_Reference --overwrite

SchemaZen.exe script --server UCLICKSERVER06\REPORTSERVER  --database UC_Report --scriptDir G:\Uclick_Product_Suite\CodeBackup\ReportServer\UC_Report --overwrite


SchemaZen.exe script --server UCLICKSERVER04\BRIDGESERVER --database UC_Bridge --scriptDir G:\Uclick_Product_Suite\CodeBackup\BridgeServer\UC_Bridge --overwrite

SchemaZen.exe script --server UCLICKSERVER01\CDRSERVER01 --database UC_Data01 --scriptDir G:\Uclick_Product_Suite\CodeBackup\CDRServer\UC_Data01 --overwrite



"G:\Uclick_Product_Suite\CodeBackup\7-Zip\7z.exe" a Celcom.zip ReferenceServer ReportServer BridgeServer CDRServer
RD /S /Q ReferenceServer ReportServer BridgeServer CDRServer