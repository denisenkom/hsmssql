module Database.Mssql.Statement where
import Database.Mssql.Tds
import Database.Mssql.Collation
import Database.Mssql.Decimal

import Control.Concurrent.MVar
import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as B
import Data.Decimal
import qualified Data.Encoding as E
import Data.Encoding.UTF16
import Data.Int
import Data.List
import Data.Ratio
import Data.String.Utils
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.LocalTime
import Database.HDBC
import Database.HDBC.Types
import System.IO
import GHC.Float

data SState =
    SState { conn :: Handle,
             squery :: String,
             coldefmv :: MVar [(String, SqlColDesc)],
             tokenstm :: MVar [Token],
             metadatatok :: MVar Token,
             bufSize :: Int}

newSth :: Handle -> Int -> String -> IO Statement
newSth conn bufSize query =
    do newcoldefmv <- newMVar []
       tokenstm <- newMVar []
       metadatatok <- newMVar TokColMetaDataEmpty
       let sstate = SState {conn = conn, squery = query,
                            coldefmv = newcoldefmv,
                            tokenstm = tokenstm,
                            metadatatok = metadatatok,
                            bufSize = bufSize}
           retval = Statement {executeRaw = fexecuteRaw sstate,
                               execute = fexecute sstate,
                               executeMany = fexecuteMany sstate,
                               getColumnNames = fgetColumnNames sstate,
                               fetchRow = ffetchRow sstate,
                               originalQuery = foriginalQuery sstate,
                               describeResult = fdescribeResult sstate,
                               finish = ffinish sstate}
       return retval

colDescFromTi :: TypeInfo -> SqlColDesc
colDescFromTi TypeInt1 = SqlColDesc SqlTinyIntT Nothing Nothing Nothing (Just False)
colDescFromTi TypeBit = SqlColDesc SqlBitT Nothing Nothing Nothing (Just False)
colDescFromTi (TypeBitN _) = SqlColDesc SqlBitT Nothing Nothing Nothing (Just True)
colDescFromTi TypeInt2 = SqlColDesc SqlSmallIntT Nothing Nothing Nothing (Just False)
colDescFromTi TypeInt4 = SqlColDesc SqlIntegerT Nothing Nothing Nothing (Just False)
colDescFromTi TypeDateTim4 = SqlColDesc SqlTimestampT Nothing Nothing Nothing (Just False)
colDescFromTi TypeFlt4 = SqlColDesc SqlFloatT Nothing Nothing Nothing (Just False)
colDescFromTi TypeMoney = SqlColDesc SqlDecimalT (Just 19) Nothing (Just 4) (Just False)
colDescFromTi TypeDateTime = SqlColDesc SqlTimestampT Nothing Nothing Nothing (Just False)
colDescFromTi TypeFlt8 = SqlColDesc SqlDoubleT Nothing Nothing Nothing (Just False)
colDescFromTi TypeMoney4 = SqlColDesc SqlDecimalT (Just 10) Nothing (Just 4) (Just False)
colDescFromTi TypeInt8 = SqlColDesc SqlBigIntT Nothing Nothing Nothing (Just False)
colDescFromTi (TypeGuid size) = SqlColDesc SqlGUIDT (Just (fromIntegral size)) (Just (fromIntegral size)) Nothing (Just True)
colDescFromTi (TypeIntN 1) = SqlColDesc SqlTinyIntT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeIntN 2) = SqlColDesc SqlSmallIntT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeIntN 4) = SqlColDesc SqlIntegerT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeIntN 8) = SqlColDesc SqlBigIntT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeFltN 4) = SqlColDesc SqlFloatT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeFltN 8) = SqlColDesc SqlDoubleT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeDecimalN prec scale) = SqlColDesc SqlDecimalT (Just (fromIntegral prec)) Nothing (Just (fromIntegral scale)) (Just True)
colDescFromTi (TypeNumericN prec scale) = SqlColDesc SqlDecimalT (Just (fromIntegral prec)) Nothing (Just (fromIntegral scale)) (Just True)
colDescFromTi (TypeMoneyN 8) = SqlColDesc SqlDecimalT (Just 19) Nothing (Just 4) (Just True)
colDescFromTi (TypeMoneyN 4) = SqlColDesc SqlDecimalT (Just 10) Nothing (Just 4) (Just True)
colDescFromTi (TypeDateTimeN _) = SqlColDesc SqlTimestampT Nothing Nothing Nothing (Just True)
colDescFromTi TypeDateN = SqlColDesc SqlDateT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeTimeN _) = SqlColDesc SqlTimeT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeDateTime2N _) = SqlColDesc SqlTimestampT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeDateTimeOffsetN _) = SqlColDesc SqlTimestampWithZoneT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeVarBinary 0xffff) = SqlColDesc SqlLongVarBinaryT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeVarBinary size) = SqlColDesc SqlVarBinaryT (Just (fromIntegral size)) (Just (fromIntegral size)) Nothing (Just True)
colDescFromTi (TypeVarChar 0xffff _) = SqlColDesc SqlLongVarCharT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeVarChar size _) = SqlColDesc SqlVarCharT (Just (fromIntegral size)) (Just (fromIntegral size)) Nothing (Just True)
colDescFromTi (TypeBinary size) = SqlColDesc SqlBinaryT (Just (fromIntegral size)) (Just (fromIntegral size)) Nothing (Just True)
colDescFromTi (TypeChar size _) = SqlColDesc SqlCharT (Just (fromIntegral size)) (Just (fromIntegral size)) Nothing (Just True)
colDescFromTi (TypeNVarChar 0xffff _) = SqlColDesc SqlWLongVarCharT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeNVarChar size _) = SqlColDesc SqlWVarCharT (Just (fromIntegral size)) (Just (fromIntegral size)) Nothing (Just True)
colDescFromTi (TypeNChar size _) = SqlColDesc SqlWCharT (Just (fromIntegral size)) (Just (fromIntegral size)) Nothing (Just True)
colDescFromTi TypeXml = SqlColDesc (SqlUnknownT "xml") Nothing Nothing Nothing (Just True)
colDescFromTi (TypeUdt size) = SqlColDesc (SqlUnknownT "udt") Nothing (Just (fromIntegral size)) Nothing (Just True)
colDescFromTi (TypeText _ _) = SqlColDesc SqlLongVarCharT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeImage _) = SqlColDesc SqlLongVarBinaryT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeNText _ _) = SqlColDesc SqlWLongVarCharT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeVariant size) = SqlColDesc (SqlUnknownT "sql_variant") Nothing (Just (fromIntegral size)) Nothing (Just True)

timeOfDayToSec :: TimeOfDay -> Rational
timeOfDayToSec = toRational . timeOfDayToTime

dateToDays :: Day -> Int32
dateToDays val = fromIntegral $ diffDays val (fromGregorian 1 1 1)

zonedTimeToTds :: ZonedTime -> TdsValue
zonedTimeToTds val = TdsDateTimeOffset days seconds zoneMinutes
    where utct = zonedTimeToUTC val
          days = dateToDays $ utctDay utct
          seconds = toRational $ utctDayTime utct
          zoneMinutes = fromIntegral $ timeZoneMinutes $ zonedTimeZone val


sqlToTdsParam :: SqlValue -> TdsValue
sqlToTdsParam val = case val of
    SqlString val -> TdsNVarCharMax emptyCollation (encodeUcs2 val)
    SqlByteString val -> TdsVarBinaryMax $ B.fromChunks [val]
    SqlWord32 val -> TdsInt8 $ fromIntegral val
    SqlWord64 val -> TdsDecimal $ fromIntegral val
    SqlInt32 val -> TdsInt4 val
    SqlInt64 val -> TdsInt8 val
    SqlInteger val -> TdsDecimal $ fromIntegral val
    SqlChar val -> TdsNChar emptyCollation (encodeUcs2Strict [val])
    SqlBool val -> TdsBool val
    SqlDouble val -> TdsFloat val
    SqlRational val -> TdsDecimal $ rationalToDec val
    SqlLocalDate val -> TdsDate $ dateToDays val
    SqlLocalTimeOfDay val -> TdsTime $ timeOfDayToSec val
    SqlLocalTime val -> TdsDateTime2 (dateToDays $ localDay val)
                                     (timeOfDayToSec $ localTimeOfDay val)
    SqlZonedTime val -> zonedTimeToTds val
    SqlNull -> TdsNull

sqlToTdsTi :: SqlValue -> TypeInfo
sqlToTdsTi val = case val of
    SqlString _ -> TypeNVarChar 0xffff emptyCollation
    SqlByteString _ -> TypeVarBinary 0xffff
    SqlWord32 _ -> TypeIntN 8
    SqlWord64 _ -> TypeDecimalN 38 0
    SqlInt32 _ -> TypeIntN 4
    SqlInt64 _ -> TypeIntN 8
    SqlInteger _ -> TypeDecimalN 38 0
    SqlChar _ -> TypeNChar 1 emptyCollation
    SqlBool _ -> TypeBitN 1
    SqlDouble _ -> TypeFltN 8
    SqlRational val -> TypeDecimalN 38 (rationalScale val)
    SqlLocalDate _ -> TypeDateN
    SqlLocalTimeOfDay _ -> TypeTimeN 7
    SqlLocalTime _ -> TypeDateTime2N 7
    SqlZonedTime _ -> TypeDateTimeOffsetN 7
    SqlNull -> TypeNVarChar 1 emptyCollation

processResp :: [Token] -> [Token] -> (Maybe Token, [Token], [Token], Bool)
processResp (metadata@(TokColMetaData _ _):xs) errors =
    (Just metadata, xs, [], True)
processResp (err@(TokError _ _ _ _ _ _ _):xs) errors =
    processResp xs (err:errors)
processResp (done@(TokDone status _ _):xs) errors =
    if status .&. doneMoreResults == 0
         then if xs == []
             then (Nothing, [], errors, isSuccess status)
             else error "unexpected tokens after final DONE token"
         else processResp xs []
processResp (done@(TokDoneProc status _ _):xs) errors =
    if status .&. doneMoreResults == 0
         then if xs == []
             then (Nothing, [], errors, isSuccess status)
             else error "unexpected tokens after final DONE token"
         else processResp xs []
processResp (tok:_) _ =
    error $ "unexpected token " ++ (show tok)
processResp [] errors =
    error $ "unexpected end of tokens"

fexecuteRaw :: SState -> IO ()
fexecuteRaw sstate =
    do tokens <- exec (conn sstate) (squery sstate) (bufSize sstate)
       let (mmetadata, remtokens, errors, status) = processResp tokens []
       case mmetadata of
           Just metadata -> do
               let metadataCols (TokColMetaData cols _) = cols
                   cols = metadataCols metadata
                   coldef (ColMetaData usertype flags ti name) =
                       (name, colDescFromTi ti)
               swapMVar (coldefmv sstate) [coldef col | col <- cols]
               swapMVar (tokenstm sstate) remtokens
               swapMVar (metadatatok sstate) metadata
               return ()
           Nothing -> do
               throwSqlError (if errors == []
                   then SqlError {seState = "", seNativeError = -1, seErrorMsg = "Query failed, server didn't send an error"}
                   else let (TokError errno _ _ msg _ _ _) = head errors
                       in SqlError {seState = "", seNativeError = errno, seErrorMsg = msg})

convertVal :: TdsValue -> SqlValue
convertVal TdsNull = SqlNull
convertVal (TdsInt1 v) = SqlInt32 (fromIntegral v)
convertVal (TdsInt2 v) = SqlInt32 (fromIntegral v)
convertVal (TdsInt4 v) = SqlInt32 v
convertVal (TdsInt8 v) = SqlInt64 v
convertVal (TdsFloat v) = SqlDouble v
convertVal (TdsReal v) = SqlDouble (float2Double v)
convertVal (TdsGuid v) = SqlByteString v
convertVal (TdsBool v) = SqlBool v
convertVal (TdsDecimal v) = SqlRational $ toRational v
convertVal (TdsMoney v) = SqlRational $ (fromIntegral v) % 10000
convertVal (TdsSmallMoney v) = SqlRational $ (fromIntegral v) % 10000
convertVal (TdsDateTime days timefrac) = SqlLocalTime time
    where day = addDays (fromIntegral days) (fromGregorian 1900 1 1)
          picoseconds = round $ ((fromIntegral timefrac) * 10^12) % 300
          daytime = timeToTimeOfDay $ picosecondsToDiffTime picoseconds
          time = LocalTime day daytime

convertVal (TdsSmallDateTime days minutes) = SqlLocalTime time
    where day = addDays (fromIntegral days) (fromGregorian 1900 1 1)
          daytime = timeToTimeOfDay $ secondsToDiffTime ((fromIntegral minutes) * 60)
          time = LocalTime day daytime

convertVal (TdsDate days) = SqlLocalDate day
    where day = addDays (fromIntegral days) (fromGregorian 1 1 1)

convertVal (TdsTime secs) = SqlLocalTimeOfDay daytime
    where picoseconds = round $ secs * 10^12
          daytime = timeToTimeOfDay $ picosecondsToDiffTime picoseconds

convertVal (TdsDateTime2 days secs) = SqlLocalTime $ LocalTime day daytime
    where day = addDays (fromIntegral days) (fromGregorian 1 1 1)
          picoseconds = round $ secs * 10^12
          daytime = timeToTimeOfDay $ picosecondsToDiffTime picoseconds

convertVal (TdsDateTimeOffset days secs offset) = res
    where startday = (fromGregorian 1 1 1)
          day = addDays (fromIntegral days) startday
          picoseconds = round $ secs * 10^12
          daytime = picosecondsToDiffTime picoseconds
          tz = minutesToTimeZone $ fromIntegral offset
          utctime = UTCTime day daytime
          zonedtime = utcToZonedTime tz utctime
          res = SqlZonedTime zonedtime

convertVal (TdsVarBinary bs) = SqlByteString bs
convertVal (TdsVarBinaryMax bs) = SqlByteString (BS.concat . B.toChunks $ bs)
convertVal (TdsBinary bs) = SqlByteString bs
convertVal (TdsChar collation bs) = SqlString $ E.decodeStrictByteString (getCharSet collation) bs
convertVal (TdsVarChar collation bs) = SqlString $ E.decodeStrictByteString (getCharSet collation) bs
convertVal (TdsVarCharMax collation bs) = SqlString $ E.decodeLazyByteString (getCharSet collation) bs
convertVal (TdsNChar collation bs) = SqlString $ E.decodeStrictByteString UTF16LE bs
convertVal (TdsNVarChar collation bs) = SqlString $ E.decodeStrictByteString UTF16LE bs
convertVal (TdsNVarCharMax collation bs) = SqlString $ E.decodeLazyByteString UTF16LE bs
convertVal (TdsXml bs) = SqlString $ E.decodeLazyByteString UTF16LE bs
convertVal (TdsText collation bs) = SqlString $ E.decodeLazyByteString (getCharSet collation) bs
convertVal (TdsNText collation bs) = SqlString $ E.decodeLazyByteString UTF16LE bs
convertVal (TdsImage bs) = SqlByteString $ B.toStrict bs

convertVals :: [TdsValue] -> [SqlValue]
convertVals [] = []
convertVals (v:xs) = convertVal v : convertVals xs

decodeRow :: Token -> [SqlValue]
decodeRow (TokRow vals) = convertVals vals

fgetColumnNames :: SState -> IO [(String)]
fgetColumnNames sstate =
    do c <- readMVar (coldefmv sstate)
       return (map fst c)


ffetchRow :: SState -> IO (Maybe [SqlValue])
ffetchRow sstate =
    do metadata <- readMVar (metadatatok sstate)
       case metadata of
            TokColMetaData _ [] -> return Nothing
            TokColMetaData cols rows -> do
                let row = head rows
                    remrows = tail rows
                swapMVar (metadatatok sstate) (TokColMetaData cols remrows)
                return $ (Just . decodeRow) row
            otherwise -> return Nothing

fexecute :: SState -> [SqlValue] -> IO Integer
fexecute sstate params = do
    let headers = [DataStmTransDescrHdr 0 1]
        s = conn sstate
        paramConv val i = Param {name = ("@p" ++ (show i)),
                                 value = sqlToTdsParam val,
                                 typeInfo = sqlToTdsTi val,
                                 flags = 0}
        tdsparams = [paramConv param i | (i, param) <- zip [1..] params]
        statementparam = Param {name = "",
                                value = TdsNVarCharMax emptyCollation (encodeUcs2 (squery sstate)),
                                typeInfo = TypeNVarChar 0xffff emptyCollation,
                                flags = 0}
        paramDecl param = name param ++ " " ++ (getDecl . typeInfo) param
        decl = join "," (map paramDecl tdsparams)
        declparam = Param {name = "",
                           value = TdsNVarCharMax emptyCollation (encodeUcs2 decl),
                           typeInfo = TypeNVarChar 0xffff emptyCollation,
                           flags = 0}
        allparams = [statementparam, declparam] ++ tdsparams
        putExecuteSql = putRpc headers SpExecuteSql 0 allparams
    sendPacketLazy (bufSize sstate) s packRPCRequest putExecuteSql
    tokens <- recvPacketLazy s getTokens
    let (mmetadata, remtokens, errors, status, procstat) = processExecResp tokens [] 1
    case mmetadata of
        Just metadata -> do
            let metadataCols (TokColMetaData cols _) = cols
                cols = metadataCols metadata
                coldef (ColMetaData usertype flags ti name) =
                    (name, colDescFromTi ti)
            swapMVar (coldefmv sstate) [coldef col | col <- cols]
            swapMVar (tokenstm sstate) remtokens
            swapMVar (metadatatok sstate) metadata
            return ()
        Nothing -> do
            throwSqlError (if errors == []
                then SqlError {seState = "", seNativeError = -1, seErrorMsg = "Query failed, server didn't send an error"}
                else let (TokError errno _ _ msg _ _ _) = head errors
                    in SqlError {seState = "", seNativeError = errno, seErrorMsg = msg})
    return 0

processExecResp :: [Token] -> [Token] -> Int -> (Maybe Token, [Token], [Token], Bool, Int)
processExecResp (metadata@(TokColMetaData _ _):xs) errors _ =
    (Just metadata, xs, [], True, 0)
processExecResp (err@(TokError _ _ _ _ _ _ _):xs) errors procstat =
    processExecResp xs (err:errors) procstat
processExecResp (done@(TokDone status _ _):xs) errors procstat =
    if status .&. doneMoreResults == 0
         then if xs == []
             then (Nothing, [], errors, isSuccess status, procstat)
             else error "unexpected tokens after final DONE token"
         else processExecResp xs [] procstat
processExecResp (done@(TokDoneProc status _ _):xs) errors procstat =
    if status .&. doneMoreResults == 0
         then if xs == []
             then (Nothing, [], errors, isSuccess status, procstat)
             else error "unexpected tokens after final DONE token"
         else processExecResp xs [] procstat
processExecResp ((TokReturnStatus status):xs) errors _ =
    processExecResp xs errors status

processExecResp (tok:_) _ _ =
    error $ "unexpected token " ++ (show tok)
processExecResp [] errors _ =
    error $ "unexpected end of tokens"

fexecuteMany :: SState -> [[SqlValue]] -> IO ()
fexecuteMany sstate params = return ()

foriginalQuery :: SState -> String
foriginalQuery sstate = squery sstate

fdescribeResult :: SState -> IO [(String, SqlColDesc)]
fdescribeResult sstate = readMVar (coldefmv sstate)

ffinish :: SState -> IO ()
ffinish sstate = return ()
