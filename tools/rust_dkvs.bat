@echo off
@rem..\target\debug\rust_dkvs.exe %1 %2 %3 %4 %5 %6 > stdout%1.txt 2>&1
..\target\release\rust_dkvs.exe %1 %2 %3 %4 %5 %6 > stdout%1.txt 2>&1

