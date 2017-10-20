@echo off

if ["%~1"]==[""] goto usage

set clarus_dir=%~dp0
set extension_dirs=%clarus_dir%\ext-libs
set options=
set security_policy_set=false
set server_address_set=false

REM Parse arguments
:args_loop
if ["%~1"]==[""] goto verify_options

REM Parse extension directories options
:parse_extension_dirs_options
if [%1]==[-xd] goto extension_dirs
if [%1]==[--extension-dirs] goto extension_dirs
goto parse_security_policy_options
:extension_dirs
set extension_dirs=%2
shift
shift
goto args_loop
 
REM Parse security policy options
:parse_security_policy_options
if [%1]==[-sp] goto security_policy
if [%1]==[--security-policy] goto security_policy
goto parse_server_address
:security_policy
set security_policy_set=true
set options=%options% %1 %2
shift
shift
goto args_loop

REM Parse server address
:parse_server_address
set server_address_set=true
goto parse_other_options

REM Parse other options
:parse_other_options
set options=%options% %1

shift
goto args_loop

REM Verify mandatory options are set
:verify_options
if [%security_policy_set%]==[false] goto usage
if [%server_address_set%]==[false] goto usage
 
REM Run CLARUS proxy
:run
java -Djava.ext.dirs=%extension_dirs% -jar %clarus_dir%\libs\proxy-main-1.0.1.jar %options%
goto end

:usage
echo usage: proxy.bat [OPTION]... [SERVER_ADDRESS]...
echo CLARUS extensions:
echo   [-xd, --extension-dirs ^<PATHS^>] list the extensions directories that contain protection modules and protocol plugins.
echo                                   Extensions directories must be separated by ';'
echo Security policy options:
echo   -sp, --security-policy ^<PATH^>   security policy to apply.
echo Resource consumption options:
echo   [-mf, --max-frame-len ^<MAX_FRAME_LENGTH^>]
echo                                   maximum frame length to process
echo   [-lt, --nb-listen-threads ^<NB_LISTEN_THREADS^>]
echo                                   number of listen threads (default: 1).
echo                                   Must be a positive number or the special value 'cores' (number of cores)
echo   [-st, --nb-session-threads ^<NB_SESSION_THREADS^>]
echo                                   number of session threads (default: number of cores).
echo                                   Must be a positive number or the special value 'cores' (number of cores)
echo   [-pt, --nb-parser-threads ^<NB_PARSER_THREADS^>]
echo                                   number of parser threads (default: 0).
echo                                   Must be a positive number or 0 or the special value 'cores' (number of cores)
echo Server addresses:
echo   ^<HOSTNAME^>[:^<PORT^>]             server host and optional port. Multiple server addresses can be specified

:end
