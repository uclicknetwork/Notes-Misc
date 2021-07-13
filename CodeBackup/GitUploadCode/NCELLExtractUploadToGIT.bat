:: Backup to GIT
@echo off

:: set Account name

set accountName=NCELL

:: set local repository path

set backupPath=C:\Users\babas\Desktop\DATA\GIT\AGS_Investor\GITDesktopUI\%accountName%\


:: set path and name of the backup zip

set ZIPBackups=C:\Users\babas\Desktop\DATA\GIT\AGS_Investor\GITDesktopUI\ZIP_Backups
set backup=%accountName%.zip

:: Extract backup

"c:\Program Files\7-Zip\7z.exe" -y x %ZIPBackups%\%backup% -o%backupPath% 


:: Upload to github repository

cd %backupPath%
git add *
git commit -m "incremental changes %date% %time%"
git push