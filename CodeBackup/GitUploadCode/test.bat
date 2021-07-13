@echo off

set accountName=Celcom
set backupPath=C:\Users\babas\Desktop\DATA\GIT\AGS_Investor\%accountName%\
set ZIPBackups=C:\Users\babas\Desktop\DATA\GIT\AGS_Investor\GITDesktopUI\ZIP_Backups
set backup=%accountName%.zip



:: Upload to github repository

cd %backupPath%
echo %backupPath%