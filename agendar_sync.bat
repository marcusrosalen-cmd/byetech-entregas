@echo off
REM ─────────────────────────────────────────────────────────
REM  agendar_sync.bat — Registra o sync no Windows Task Scheduler
REM  Execute UMA VEZ como Administrador para agendar o sync diário.
REM ─────────────────────────────────────────────────────────

SET PASTA=%~dp0
SET PYTHON=python
SET SCRIPT=%PASTA%sync_lovable.py

echo.
echo Registrando sync diario no Windows Task Scheduler...
echo Pasta: %PASTA%
echo.

REM Remove tarefa antiga se existir
schtasks /delete /tn "BytechSync" /f 2>nul

REM Cria nova tarefa — roda todo dia às 08:00
schtasks /create ^
  /tn "BytechSync" ^
  /tr "\"%PYTHON%\" \"%SCRIPT%\" --fonte completo" ^
  /sc DAILY ^
  /st 08:00 ^
  /ru "%USERNAME%" ^
  /rl HIGHEST ^
  /f

echo.
echo ✅ Tarefa "BytechSync" criada com sucesso!
echo    Roda todo dia às 08:00 automaticamente.
echo.
echo Para verificar: Painel de Controle → Agendador de Tarefas → BytechSync
echo Para rodar agora: python sync_lovable.py
echo.
pause
